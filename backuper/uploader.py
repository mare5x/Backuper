import os
import queue
from concurrent.futures import ThreadPoolExecutor

from pytools import filetools as ft

from . import database as db


class UploadQueue(queue.Queue):
    pass   


class DriveUploader:
    """Manages synchronized multi threaded file uploading to Google Drive."""

    def __init__(self, settings, google):
        self.settings = settings
        self.google = google

    def get_root_folder_id(self):
        folder_id = self.settings.data_file.get_root_folder_id()
        if folder_id is None:
            if not self.google.get_file_data_by_name("Backuper"):
                folder_id = self.google.create_folder("Backuper")
            else:
                folder_id = self.google.get_file_data_by_name("Backuper")[0]['id']
            self.settings.set_root_folder_id(folder_id)
        return folder_id

    def get_parent_folder_id(self, entry):
        folder_id = db.GoogleDriveDB.get_parent_folder_id(entry)
        if folder_id is None:
            folder_id = self.get_root_folder_id()
        return folder_id

    def upload_file(self, path, folder_id=None):
        entry = db.unify_path(path)
        if folder_id is None:
            folder_id = self.get_parent_folder_id(entry)
        file_id = db.GoogleDriveDB.get_stored_path_id(entry)
        resp = self.google.upload_file(path, folder_id=folder_id, file_id=file_id)
        db.GoogleDriveDB.create_or_update(path=entry, drive_id=resp['id'], 
            date_modified_on_disk=ft.date_modified(entry), md5sum=ft.md5sum(entry))
        return resp['id']

    def create_dir(self, path):
        entry = db.unify_path(path)
        folder_id = db.GoogleDriveDB.get_stored_path_id(entry)
        if folder_id is None:
            parent_id = self.get_parent_folder_id(entry)
            folder_id = self.google.create_folder(ft.real_case_filename(entry), parent_id=parent_id)
            db.GoogleDriveDB.create(path=entry, drive_id=folder_id, 
                date_modified_on_disk=ft.date_modified(entry), md5sum=db.GoogleDriveDB.FOLDER_MD5)
        return folder_id

    def start_upload_queue(self, n_threads=5):
        """N threads will upload items from a queue, until the queue is empty.

        Returns an UploadQueue object. Populate the queue using the queue's put() method.
        The file paths in the queue should be valid (not blacklisted).
        When done, call wait_for_queue(q).

        Make sure the folder structure has already been created, before uploading nested
        files.
        """
        q = UploadQueue()
        q.n_threads = n_threads  # A convenience attribute.
        executor = ThreadPoolExecutor(max_workers=n_threads, thread_name_prefix="DriveUploader")
        for i in range(n_threads):
            executor.submit(self.upload_queue_worker, q)
        # The resources associated with the executor will be freed when all pending futures are done executing.
        executor.shutdown(wait=False)
        return q

    def upload_queue_worker(self, q):
        while True:
            entry = q.get()
            if entry is None:
                q.task_done()
                break

            if os.path.isdir(entry):
                self.create_dir(entry)
            else:
                self.upload_file(entry)

            q.task_done()

    def wait_for_queue(self, q):
        """q must be an UploadQueue returned by the start_upload_queue method.
        After this method returns, consider the queue unusable. 
        """
        # Block until all tasks are done.
        q.join()

        # Stop worker threads.
        for _ in range(q.n_threads):
            q.put(None)
        