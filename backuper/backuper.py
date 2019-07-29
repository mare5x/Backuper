import os
import tempfile
import logging
import concurrent.futures

from pytools import filetools as ft

from . import settings, googledrive, uploader, downloader, filecrawler, treelog, database

# Guarantee: no files will be deleted from the local file system!

SETTINGS_FILE = "_settings.ini"
DATA_FILE = "_backuper.ini"


class Backuper:
    def __init__(self, pretty_log=False):
        database.GoogleDriveDB.init()
        self.conf = settings.Settings(SETTINGS_FILE, DATA_FILE)
        
        if pretty_log:
            dirpath = ft.create_dir("logs")
            name = "BackuperPP_{}.log".format(ft.get_current_date_string())
            path = os.path.join(dirpath, name)
            self.google = googledrive.PPGoogleDrive(filename=path, mode="a")
            logging.info("PrettyPrint log file: %s", path)
            print(path)
        else:
            self.google = googledrive.GoogleDrive()

    def __enter__(self):
        return self
    
    def __exit__(self, *exc):
        self.exit()

    def exit(self):
        self.google.exit()
        self.conf.exit()
        database.GoogleDriveDB.close()

    def list_upload_changes(self):
        print("Listing changes to upload ...")
        file_crawler = filecrawler.LocalFileCrawler(self.conf)
        for dirpath in self.conf.sync_dirs:
            for path in file_crawler.get_all_paths_to_sync(dirpath):
                print(path)

    def upload_changes(self):
        print("Uploading changes ...")
        THREADS = 5
        gd_uploader = uploader.DBDriveUploader(self.conf, self.google)
        # First, the folder structure must be made so that files can be placed
        # in the correct directories. This can't be queued because the order is 
        # important.
        with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS, thread_name_prefix="Backuper") as ex:
            futures = []
            for dirpath in self.conf.sync_dirs:
                futures.append(ex.submit(self._upload_folder_structure, dirpath, gd_uploader))
            for fut in concurrent.futures.as_completed(futures):
                fut.result()  # Re-raise the exception, if it occurred once all threads are done.

        # Now, we can upload the files.
        q = gd_uploader.start_upload_queue(n_threads=THREADS)
        for dirpath in self.conf.sync_dirs:
            self._enqueue_path_changes(dirpath, q)
        gd_uploader.wait_for_queue(q)

        self.conf.data_file.set_last_upload_time()

    def _upload_folder_structure(self, dirpath, gd_uploader):
        file_crawler = filecrawler.LocalFileCrawler(self.conf)
        for folder in file_crawler.get_folders_to_sync(dirpath):
            gd_uploader.create_dir(folder)

    def _enqueue_path_changes(self, dirpath, q): 
        file_crawler = filecrawler.LocalFileCrawler(self.conf)
        for fpath in file_crawler.get_files_to_sync(dirpath):
            q.put(fpath)

    def list_download_changes(self):
        print("Listing changes to download ...")
        crawler = filecrawler.DriveFileCrawler(self.conf, self.google)
        for obj in crawler.get_changes_to_download(update_token=False):
            decision = "CONFLICT" if obj.sync_decision == crawler.CONFLICT_FLAG else "SAFE TO DL"
            print(decision, obj.file_id, obj.remote_path)

    def download_changes(self):
        print("Downloading changes ...")
        THREADS = 8
        db = database.GoogleDriveDB()
        crawler = filecrawler.DriveFileCrawler(self.conf, self.google)
        gd_downloader = downloader.DriveDownloader(self.google)
        Entry = gd_downloader.DLQEntry

        root_dl_path = self.conf.user_settings_file.get_download_path()
        root_folder_id = self.conf.get_root_folder_id(self.google)
        root_path = self.google.get_remote_path(root_folder_id)
        
        # Map archived remote paths to local paths.
        archived_map = dict()
        for path in self.conf.sync_dirs:
            archive = db.get("path", path)
            if archive is None: continue
            remote_path = self.google.get_remote_path(archive.drive_id)
            archived_map[remote_path] = archive.path
        archived_map[root_path] = root_dl_path

        def get_dl_path(remote_path):
            head, tail = os.path.split(remote_path)
            while head != root_path:
                if head in archived_map: break
                head, tmp = os.path.split(head)
                tail = os.path.join(tmp, tail)
            return os.path.join(archived_map[head], tail)

        q = gd_downloader.start_download_queue(n_threads=THREADS)
        for obj in crawler.get_changes_to_download(root_path, update_token=True):
            if obj.sync_decision == crawler.CONFLICT_FLAG:
                print("CONFLICT", obj)
                continue
            path = get_dl_path(obj.remote_path)
            args = { "type": obj.type, "file_id": obj.file_id, "path": path }
            if obj.type == "#file":
                path, filename = os.path.split(path)
                args.update( {'path': path, 'filename': filename, 'md5sum': obj.md5checksum} )
            q.put(Entry(**args))
        gd_downloader.wait_for_queue(q)

        self.conf.data_file.set_last_download_sync_time()

    def sync_changes(self): pass

    def download_path_changes(self): pass

    def sync_path_changes(self): pass

    def mirror(self): pass

    def list_removed_from_gd(self):
        print("Listing files removed from Google Drive ...")
        db = database.GoogleDriveDB()
        crawler = filecrawler.DriveFileCrawler(self.conf, self.google)
        for removed_file_id in crawler.get_last_removed(update_token=False):
            archive = db.get("drive_id", removed_file_id)
            if archive:
                print(archive.path, archive.drive_id)

    def blacklist_removed_from_gd(self):
        print("Blacklisting files removed from Google Drive ...")
        # Reason: if a file is removed from GD, we don't want to reupload it.
        db = database.GoogleDriveDB()
        crawler = filecrawler.DriveFileCrawler(self.conf, self.google)
        for removed_file_id in crawler.get_last_removed(update_token=True):
            archive = db.get("drive_id", removed_file_id)
            if archive:
                # If a folder got removed, all children got removed as well.
                # However, only the root directory needs to be blacklisted.
                self.conf.blacklist_path(archive.path)
                model = database.DriveArchive
                q = model.delete().where(model.path.contains(archive.path))
                q.execute()
        self.conf.clean_blacklisted_paths()
        # TODO: use the database instead of the data file to store the blacklist.

    def upload_tree_logs_zip(self):
        print("Creating and uploading trees ...")
        zip_path = treelog.create_tree_logs_zip(self.conf, ".")
        gd_uploader = uploader.DriveUploader(self.conf, self.google)
        root_id = self.conf.get_root_folder_id(self.google)
        tree_folder_id = treelog.get_or_create_tree_folder_id(self.conf, self.google, root_id)
        gd_uploader.upload_file(zip_path, folder_id=tree_folder_id)
        ft.remove_file(zip_path)
