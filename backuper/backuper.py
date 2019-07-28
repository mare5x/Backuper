import os
import tempfile
import logging
import concurrent.futures

from pytools import filetools as ft

from . import settings, googledrive, uploader, filecrawler, treelog, database

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
        file_crawler = filecrawler.LocalFileCrawler(self.conf)
        for dirpath in self.conf.sync_dirs:
            for path in file_crawler.get_all_paths_to_sync(dirpath):
                print(path)

    def upload_changes(self):
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
        crawler = filecrawler.DriveFileCrawler(self.conf, self.google)
        for obj in crawler.get_changes_to_download(update_token=False):
            decision = "CONFLICT" if obj.sync_decision == crawler.CONFLICT_FLAG else "SAFE TO DL"
            print(decision, obj.file_id, self.google.get_remote_path(obj.file_id))

    def download_changes(self): pass

    def sync_changes(self): pass

    def download_path_changes(self): pass

    def sync_path_changes(self): pass

    def list_removed_from_gd(self):
        db = database.GoogleDriveDB()
        crawler = filecrawler.DriveFileCrawler(self.conf, self.google)
        for removed_file_id in crawler.get_last_removed(update_token=False):
            archive = db.get("drive_id", removed_file_id)
            if archive:
                print(archive.path, archive.drive_id)

    def blacklist_removed_from_gd(self):
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
        zip_path = treelog.create_tree_logs_zip(self.conf, ".")
        gd_uploader = uploader.DriveUploader(self.conf, self.google)
        root_id = gd_uploader.get_root_folder_id()
        tree_folder_id = treelog.get_or_create_tree_folder_id(self.conf, self.google, root_id)
        gd_uploader.upload_file(zip_path, folder_id=tree_folder_id)
        ft.remove_file(zip_path)
