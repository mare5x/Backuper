import os
from collections import namedtuple

from pytools import filetools as ft

from . import database as db
from . import _loader


class UploadQueue(_loader._Queue): pass


class DriveUploader:
    """Synchronized multi threaded file uploading to Google Drive."""

    # folder_id [None] and file_id [None] are optional fields.
    DUQEntry = namedtuple("DUQEntry", ["path", "folder_id", "file_id"], defaults=[None, None])

    def __init__(self, settings, google):
        self.settings = settings
        self.google = google

    def get_root_folder_id(self):
        folder_id = self.settings.data_file.get_root_folder_id()
        if folder_id is None:
            folder_id = self.google.create_folder("Backuper")
            self.settings.set_root_folder_id(folder_id)
        return folder_id

    def upload_file(self, path, folder_id=None, file_id=None):
        if folder_id is None:
            folder_id = self.get_root_folder_id()
        resp = self.google.upload_file(path, folder_id=folder_id, file_id=file_id)
        return resp['id']

    def create_dir(self, path, folder_name=None, parent_folder_id=None):
        if parent_folder_id is None:
            parent_folder_id = self.get_root_folder_id()
        if folder_name is None:
            folder_name = ft.real_case_filename(path)
        return self.google.create_folder(folder_name, parent_id=parent_folder_id)

    def start_upload_queue(self, n_threads=5):
        """N threads will upload items from a queue, until the queue is empty.

        Returns an UploadQueue object. Populate the queue with DUQEntry
        objects using the queue's put() method. When done, call wait_for_queue(q).

        When enqueuing files/dirs that have parents, make sure the parents 
        have already been created.
        """
        return _loader.start_queue(self.process_queue_entry, n_threads=n_threads, 
            thread_prefix="DriveUploader")

    def process_queue_entry(self, qentry):
        """Subclasses can override this function and DUQEntry's definition."""
        if os.path.isdir(qentry.path):
            self.create_dir(qentry.path, parent_folder_id=qentry.folder_id)
        else:
            self.upload_file(qentry.path, folder_id=qentry.folder_id, file_id=qentry.file_id)

    def wait_for_queue(self, q, stop=True):
        """q must be an UploadQueue returned by the start_upload_queue method.
        If 'stop' is True, consider the queue unusable. Associated threads will stop.

        Exceptions raised by threads working the queue will get raised here.
        """
        return _loader.wait_for_queue(q, stop=stop)


class DBDriveUploader(DriveUploader):
    """Database aware DriveUploader."""

    # Override. The entries are simple paths.
    DUQEntry = str 

    def __init__(self, google, settings, update_db=True):
        super().__init__(google, settings)
        self.update_db = update_db

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
        file_id = super().upload_file(entry, folder_id, file_id)
        if self.update_db:
            db.GoogleDriveDB.create_or_update(path=entry, drive_id=file_id, 
                date_modified_on_disk=ft.date_modified(entry), md5sum=ft.md5sum(entry))
        return file_id

    def create_dir(self, path):
        entry = db.unify_path(path)
        folder_id = db.GoogleDriveDB.get_stored_path_id(entry)
        if folder_id is None:
            parent_id = self.get_parent_folder_id(entry)
            folder_id = super().create_dir(entry, parent_folder_id=parent_id)
            if self.update_db:
                db.GoogleDriveDB.create(path=entry, drive_id=folder_id, 
                    date_modified_on_disk=ft.date_modified(entry), md5sum=db.GoogleDriveDB.FOLDER_MD5)
        return folder_id

    def process_queue_entry(self, qentry):
        """Override. The queue should be populated with valid (not blacklisted) paths."""
        if os.path.isdir(qentry):
            self.create_dir(qentry)
        else:
            self.upload_file(qentry)
