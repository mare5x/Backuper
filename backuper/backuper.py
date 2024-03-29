import os
import tempfile
import logging
import pprint
import concurrent.futures

from pytools import filetools as ft

from . import settings, googledrive, uploader, downloader, filecrawler, treelog, database, _helpers

# Guarantee: no files will be deleted from the local file system!

SETTINGS_FILE = "settings.ini"
DATA_FILE = "backuper.ini"


class Backuper:
    def __init__(self, pretty_log=False):
        database.GoogleDriveDB.init()
        self.conf = settings.Settings(SETTINGS_FILE, DATA_FILE)
        self.upload_threads = self.conf.user_settings_file.get_int("upload_threads")
        self.download_threads = self.conf.user_settings_file.get_int("download_threads")
        
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

    def _init(self):
        self.conf.get_root_folder_id(self.google)  # Create the root folder.
        self.conf.data_file.init_values(self.google)

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
        gd_uploader = uploader.DBDriveUploader(self.google, self.conf.get_root_folder_id(self.google))
        # First, the folder structure must be made so that files can be placed
        # in the correct directories. This can't be queued because the order is 
        # important.
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.upload_threads, thread_name_prefix="Backuper") as ex:
            futures = []
            for dirpath in self.conf.sync_dirs:
                futures.append(ex.submit(self._upload_folder_structure, dirpath, gd_uploader))
            for fut in concurrent.futures.as_completed(futures):
                fut.result()  # Re-raise the exception, if it occurred once all threads are done.

        # Now, we can upload the files.
        q = gd_uploader.start_upload_queue(n_threads=self.upload_threads)
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

    def handle_download_conflicts(self, conflicts, dry_run=False):
        print("Handling download conflicts ..." + (" (dry)" if dry_run else ""))
        import textwrap
        help_str = textwrap.dedent("""
        Enter:
        'y' to accept the resolution, 
        'n' to reject it,
        'o' to accept the resolution but create a non-conflicting filename,
        'a' to accept all remaining as 'y',
        'r' to reject all remaining as 'n',
        'c' to accept all remaining as 'o', and
        '?' for help.
        """)
        
        print("There are {} download conflicts.".format(len(conflicts)))

        resolved = []
        rejected = []
        accept_all = False
        reject_all = False
        accept_all_nonconflict = False
        for obj, path in conflicts:
            while True:
                q = '?'
                if accept_all:
                    q = 'y'
                elif reject_all:
                    q = 'n'
                elif accept_all_nonconflict:
                    q = 'o'
                else:
                    remote_path = self.google.get_remote_path(obj.resp["id"])
                    q = input("CONFLICT: {remote_path} ({file_id}) => {path} [y/n/o/a/r/c/?]: ".format(
                            remote_path=remote_path, file_id=obj.resp["id"], path=path))
                
                if q == 'y':
                    resolved.append((obj, path))
                    break
                elif q == 'n':
                    rejected.append((obj, path))
                    break
                elif q == 'o':
                    resolved.append((obj, ft.create_filename(path)))
                    break
                elif q == 'a':
                    accept_all = True
                elif q == 'r':
                    reject_all = True
                elif q == 'c':
                    accept_all_nonconflict = True
                elif q == '?':
                    print(help_str)
                else:
                    print("Invalid input.")
        return resolved, rejected

    def download_changes(self, dry_run=False):
        print("Downloading changes ..." + (" (dry)" if dry_run else ""))
        db = database.GoogleDriveDB()
        crawler = filecrawler.DriveFileCrawler(self.conf, self.google)
        gd_downloader = downloader.DriveDownloader(self.google)
        Entry = gd_downloader.DLQEntry

        # Map tracked ids to local paths.
        tracked_map = dict()
        for path in self.conf.sync_dirs:
            archive = db.get("path", path)
            if archive is None: continue
            tracked_map[archive.drive_id] = archive.path

        if self.conf.user_settings_file.get_bool("download_untracked_changes"):
            root_dl_path = self.conf.user_settings_file.get_path_in_option("default_download_path")
            root_folder_id = self.conf.get_root_folder_id(self.google)
            tracked_map[root_folder_id] = root_dl_path

        def get_dl_path(obj):
            # We can save one API call by using the change response ...
            file_id, parent_id, name = obj.resp["id"], obj.resp["parents"][0], obj.resp["name"]
            root_parent_id = obj.root_parent_id

            mid_path = self.google.get_remote_path(parent_id, root_parent_id).strip(os.path.sep)
            archive = db.get("drive_id", root_parent_id)
            pre_path = archive.path if archive else tracked_map[root_parent_id]
            return os.path.join(pre_path, mid_path, name)

        def enqueue(q, obj, path):
            args = { "type": obj.type, "file_id": obj.resp["id"], "path": path }
            if obj.type == "#file":
                path, filename = os.path.split(path)
                args.update( {'path': path, 'filename': filename, 'md5sum': obj.resp["md5Checksum"]} )
            if dry_run: pprint.pprint(args)
            else: q.put(Entry(**args))

        q = gd_downloader.start_download_queue(n_threads=self.download_threads)
        conflicts = []
        for obj in crawler.get_changes_to_download(set(tracked_map.keys()), update_token=(not dry_run)):
            path = get_dl_path(obj)
            # Different remote folders with the same name can co-exist remotely. However, locally
            # they cannot (on Windows). As such, the contents of those folders will be dumped
            # into the same folder locally. The best way to handle this conflict is by preventing
            # it from happening ... TODO
            if obj.sync_decision == crawler.CONFLICT_FLAG or os.path.exists(path):
                conflicts.append((obj, path))
                continue
            enqueue(q, obj, path)
        
        no_conflicts = len(conflicts) == 0
        gd_downloader.wait_for_queue(q, stop=no_conflicts)
        if not no_conflicts:
            resolved, rejected = self.handle_download_conflicts(conflicts, dry_run=dry_run)
            for obj, path in resolved:
                enqueue(q, obj, path)
            gd_downloader.wait_for_queue(q)

        # TODO: even if wait_for_queue raised an exception this code is run?!?!?
        if not dry_run:
            self.conf.data_file.set_last_download_sync_time()

    def sync_changes(self): pass

    def download_path_changes(self): pass

    def sync_path_changes(self): pass

    def mirror(self, path, folder_id=None, fast=False, dry_run=False):
        """Mirror a local path onto Google Drive.
        If fast, only the database will be mirrored. Non-archived files on GD 
        will remain. Otherwise, the mirror will be fully representative of
        the local path.
        """
        db = database.GoogleDriveDB()
        if folder_id is None:
            entry = database.unify_path(path)
            archive = db.get("path", entry)
            if archive: 
                folder_id = archive.drive_id
            else:
                # On a dry_run nothing should be created. At this point
                # we can conclude that this is the first time syncing this path
                # onto GD.
                if dry_run:
                    folder_id = "?UNKNOWN?"
                else:
                    gd_uploader = uploader.DBDriveUploader(self.google, self.conf.get_root_folder_id(self.google))
                    folder_id = gd_uploader.create_dir(entry)

        print("Mirror {} => {} ...".format(path, folder_id) + (" (dry)" if dry_run else ""))

        if fast:
            _helpers.delete_removed_from_local_db(self.google, path, dry_run=dry_run)
        else:
            # It would be much faster to just list all files newer than a given age
            # and check if they are in the correct folder ...
            _helpers.delete_nonlocal_in_gd(self.google, folder_id, dry_run=dry_run)
        self.full_upload_sync(folder_id, path, dry_run=dry_run)

    def mirror_all(self, fast=False, dry_run=False):
        # Performance idea: use a UFDS (union find disjoint set).
        print("Mirroring all sync_dirs (settings.ini) ...")
        for path in self.conf.sync_dirs:
            self.mirror(path, fast=fast, dry_run=dry_run)

    def full_upload_sync(self, folder_id, local_path, dry_run=False):
        if not os.path.exists(local_path): return

        print("Full upload sync {} => {} ...".format(local_path, folder_id) + (" (dry)" if dry_run else ""))

        gd_uploader = uploader.DBDriveUploader(self.google, folder_id)
        file_crawler = filecrawler.LocalFileCrawler(self.conf)
        
        # Link folder_id and local_path manually, so that no new base folder
        # is created inside folder_id.
        if not dry_run:
            entry = database.unify_path(local_path)
            _db = database.GoogleDriveDB
            _db.create_or_update(path=entry, drive_id=folder_id, 
                date_modified_on_disk=ft.date_modified(entry), md5sum=_db.FOLDER_MD5)

        for folder in file_crawler.get_folders_to_sync(local_path):
            if dry_run: print(folder)
            else: gd_uploader.create_dir(folder)

        q = gd_uploader.start_upload_queue(n_threads=self.upload_threads)
        for fpath in file_crawler.get_files_to_sync(local_path):
            if dry_run: print(fpath)
            else: q.put(fpath)
        gd_uploader.wait_for_queue(q)

    def full_download_sync(self, folder_id, local_path, dry_run=False):
        print("Full download sync {} => {} ...".format(folder_id, local_path) + (" (dry)" if dry_run else ""))

        gd_downloader = downloader.DriveDownloader(self.google)
        crawler = filecrawler.DriveFileCrawler(self.conf, self.google)
        Entry = gd_downloader.DLQEntry
        
        def enqueue(q, obj, path):
            args = { "type": obj.type, "file_id": obj.file_id, "path": path }
            if obj.type == "#file":
                path, filename = os.path.split(path)
                args.update( {'path': path, 'filename': filename, 'md5sum': obj.md5checksum} )
            if dry_run: pprint.pprint(args)
            else: q.put(Entry(**args))

        q = gd_downloader.start_download_queue(n_threads=self.download_threads)
        conflicts = []
        for obj in crawler.get_ids_to_download_in_folder(folder_id):
            # Get rid of the folder name prefix, so that local_path is the 
            # destination folder of items inside folder_id.
            remote_path = obj.remote_path.split(os.path.sep, 1)[1] if os.path.sep in obj.remote_path else ""
            dl_path = os.path.join(local_path, remote_path)
            if obj.sync_decision == crawler.CONFLICT_FLAG or os.path.exists(dl_path):
                conflicts.append((obj, dl_path))
                continue
            enqueue(q, obj, dl_path)
        
        no_conflicts = len(conflicts) == 0
        gd_downloader.wait_for_queue(q, stop=no_conflicts)
        if not no_conflicts:
            resolved, rejected = self.handle_download_conflicts(conflicts, dry_run=dry_run)
            for obj, path in resolved:
                enqueue(q, obj, path)
            gd_downloader.wait_for_queue(q)

    def full_folder_sync(self, folder_id, local_path, dry_run=False):
        print("Fully syncing: {} <=> {} ...".format(folder_id, local_path) + (" (dry)" if dry_run else ""))
        self.full_upload_sync(folder_id, local_path, dry_run=dry_run)
        self.full_download_sync(folder_id, local_path, dry_run=dry_run)

    def get_removed_from_gd(self, update_token):
        db = database.GoogleDriveDB()
        crawler = filecrawler.DriveFileCrawler(self.conf, self.google)
        for removed_file_id in crawler.get_last_removed(update_token=update_token):
            archive = db.get("drive_id", removed_file_id)
            if archive:
                yield archive

    def list_removed_from_gd(self):
        print("Listing files removed from Google Drive ...")
        for archive in self.get_removed_from_gd(False):
            print(archive.path, archive.drive_id)

    def blacklist_removed_from_gd(self):
        print("Blacklisting files removed from Google Drive ...")
        # Reason: if a file is removed from GD, we don't want to reupload it.
        for archive in self.get_removed_from_gd(True):
            print(archive.path, archive.drive_id)
            # If a folder got removed, all children got removed as well.
            # However, only the root directory needs to be blacklisted.
            self.conf.blacklist_path(archive.path)
            model = database.GoogleDriveDB.model
            q = model.delete().where(model.path.contains(archive.path))
            q.execute()
        self.conf.clean_blacklisted_paths()
        # TODO: use the database instead of the data file to store the blacklist.

    def remove_db_removed_from_gd(self):
        print("Removing files removed from Google Drive from the database ...")
        for archive in self.get_removed_from_gd(True):
            print(archive.path, archive.drive_id)
            # If a folder got removed, all children got removed as well.
            self.conf.blacklist_path(archive.path)
            model = database.GoogleDriveDB.model
            q = model.delete().where(model.path.contains(archive.path))
            q.execute()

    def upload_tree_logs_zip(self):
        print("Creating and uploading trees ...")
        user_conf = self.conf.user_settings_file
        keep_local = user_conf.get_bool("tree_keep_local")
        zip_dir_path = user_conf.get_path_in_option("tree_keep_path") if keep_local else "."

        zip_path = treelog.create_tree_logs_zip(self.conf, zip_dir_path)
        gd_uploader = uploader.DriveUploader(self.google)
        root_id = self.conf.get_root_folder_id(self.google)
        tree_folder_id = treelog.get_or_create_tree_folder_id(self.conf, self.google, root_id)
        gd_uploader.upload_file(zip_path, folder_id=tree_folder_id)
        
        if not keep_local:
            ft.remove_file(zip_path)
        else:
            print(zip_path)
