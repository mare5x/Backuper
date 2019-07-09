import tempfile
from concurrent.futures import ThreadPoolExecutor

from pytools import filetools as ft

from . import settings, googledrive, uploader, filecrawler, treelog, database

# Guarantee: no files will be deleted from the local file system!

SETTINGS_FILE = "_settings.ini"
DATA_FILE = "_backuper.ini"


class Backuper:
    def __init__(self):
        self.conf = settings.Settings(SETTINGS_FILE, DATA_FILE)
        self.google = googledrive.GoogleDrive()
        database.GoogleDriveDB.init()

    def __enter__(self):
        return self
    
    def __exit__(self, *exc):
        self.exit()

    def exit(self):
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
        with ThreadPoolExecutor(max_workers=THREADS, thread_name_prefix="Backuper") as ex:
            for dirpath in self.conf.sync_dirs:
                ex.submit(self._upload_folder_structure, dirpath, gd_uploader)
        
        # Now, we can upload the files.
        q = gd_uploader.start_upload_queue(n_threads=THREADS)
        for dirpath in self.conf.sync_dirs:
            self._enqueue_path_changes(dirpath, q)
        gd_uploader.wait_for_queue(q)

    def download_changes(self): pass

    def sync_changes(self): pass

    def _upload_folder_structure(self, dirpath, gd_uploader):
        file_crawler = filecrawler.LocalFileCrawler(self.conf)
        for folder in file_crawler.get_folders_to_sync(dirpath):
            gd_uploader.create_dir(folder)

    def _enqueue_path_changes(self, dirpath, q): 
        file_crawler = filecrawler.LocalFileCrawler(self.conf)
        for fpath in file_crawler.get_files_to_sync(dirpath):
            q.put(fpath)

    def download_path_changes(self): pass

    def sync_path_changes(self): pass

    def blacklist_removed_from_gd(self): pass

    def upload_tree_logs_zip(self):
        zip_path = treelog.create_tree_logs_zip(self.conf, ".")
        gd_uploader = uploader.DriveUploader(self.conf, self.google)
        root_id = gd_uploader.get_root_folder_id()
        tree_folder_id = treelog.get_or_create_tree_folder_id(self.conf, self.google, root_id)
        gd_uploader.upload_file(zip_path, folder_id=tree_folder_id)
        ft.remove_file(zip_path)
