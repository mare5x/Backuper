import os
from collections import namedtuple

from pytools import filetools as ft

from . import database as db
from . import _loader


class DownloadQueue(_loader._Queue): pass


class DriveDownloader:
    """Manages synchronized multi threaded file downloading from Google Drive."""
    
    DLQEntry = namedtuple("DLQEntry", ["type", "file_id", "path", "md5sum", "filename"])
    # Compatibility with Python < 3.7 (https://stackoverflow.com/questions/11351032/namedtuple-and-default-values-for-optional-keyword-arguments)
    DLQEntry.__new__.__defaults__ = ('', '')

    def __init__(self, google, update_db=True):
        self.google = google
        self.update_db = update_db

    def create_folder(self, folder_id, path):
        os.makedirs(path, exist_ok=True)
        if self.update_db:
            entry = db.unify_path(path)
            db.GoogleDriveDB.create_or_update(path=entry, drive_id=folder_id, 
                date_modified_on_disk=ft.date_modified(entry), md5sum=db.GoogleDriveDB.FOLDER_MD5)

    def download_file(self, file_id, dirpath, filename, md5sum):
        # The md5Checksum should be retrieved using the Google Drive API. 
        self.google.download_file(file_id, dirpath, filename=filename)
        if self.update_db:
            entry = db.unify_path(os.path.join(dirpath, filename))
            db.GoogleDriveDB.create_or_update(path=entry, drive_id=file_id,
                date_modified_on_disk=ft.date_modified(entry), md5sum=md5sum)

    def start_download_queue(self, n_threads=5):
        """N threads will download items from a queue, until the queue is empty.

        Returns a DownloadQueue object. Populate the queue with DLQEntry objects
        using the queue's put() method. When done, call wait_for_queue(q).
        """
        return _loader.start_queue(self.process_queue_entry, n_threads=n_threads, thread_prefix="DriveDownloader")

    def process_queue_entry(self, entry):
        if entry.type == "#folder":
            self.create_folder(entry.file_id, entry.path)
        else:  # "#file"
            self.download_file(entry.file_id, entry.path, entry.filename, entry.md5sum)

    def wait_for_queue(self, q, stop=True):
        """q must be a DownloadQueue returned by the start_download_queue method.
        If 'stop' is True, consider the queue unusable. Associated threads will stop.
        """
        return _loader.wait_for_queue(q, stop=stop)
