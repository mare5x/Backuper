import os
import queue
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor

from pytools import filetools as ft

from . import database as db


DLQEntry = namedtuple("DLQEntry", ["type", "file_id", "path", "md5sum", "filename"], defaults=['', ''])

class DownloadQueue(queue.Queue):
    pass


class DriveDownloader:
    """Manages synchronized multi threaded file downloading from Google Drive."""

    def __init__(self, google, update_db=True):
        self.google = google
        self.update_db = update_db

    def create_folder(self, folder_id, path):
        entry = db.unify_path(path)
        os.makedirs(entry, exist_ok=True)
        if self.update_db:
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
        q = DownloadQueue()
        q.n_threads = n_threads  # A convenience attribute.
        executor = ThreadPoolExecutor(max_workers=n_threads, thread_name_prefix="DriveDownloader")
        for _ in range(n_threads):
            executor.submit(self.download_queue_worker, q)
        # The resources associated with the executor will be freed when all pending futures are done executing.
        executor.shutdown(wait=False)
        return q

    def download_queue_worker(self, q):
        while True:
            entry = q.get()
            if entry is None:
                q.task_done()
                break

            if entry.type == "#folder":
                self.create_folder(entry.file_id, entry.path)
            else:  # "#file"
                self.download_file(entry.file_id, entry.path, entry.filename, entry.md5sum)

            q.task_done()

    def wait_for_queue(self, q, stop=True):
        """q must be a DownloadQueue returned by the start_download_queue method.
        If 'stop' is True, consider the queue unusable. Associated threads will stop.
        """
        # Block until all tasks are done.
        q.join()

        # Stop worker threads.
        if stop:
            for _ in range(q.n_threads):
                q.put(None)
