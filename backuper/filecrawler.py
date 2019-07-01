import os

from pytools import filetools as ft

from . import database as db


class FileCrawler:
    def __init__(self, settings):
        self.conf = settings

    def is_for_sync(self, path):
        """Note: make sure path is not blacklisted."""
        entry = db.unify_path(path)
        archive = db.db_get(db.DriveArchive, db.DriveArchive.path, entry)
        if archive is not None:
            # Folder already exists in google drive.
            return ft.date_modified(entry) > archive.date_modified_on_disk if not os.path.isdir(entry) else False
        return True

    def get_all_paths_to_sync(self, path):
        for root, dirs, files in os.walk(path):
            if self.conf.is_blacklisted(root):
                dirs.clear()
                continue
            if self.is_for_sync(root):
                yield root
            for f in files:
                f_path = os.path.join(root, f)
                if not self.conf.is_blacklisted(f_path) and self.is_for_sync(f_path):
                    yield f_path

    def get_files_to_sync(self, path):
        for root, dirs, files in os.walk(path):
            if self.conf.is_blacklisted(root):
                dirs.clear()
                continue
            for f in files:
                f_path = os.path.join(root, f)
                if not self.conf.is_blacklisted(f_path) and self.is_for_sync(f_path):
                    yield f_path

    def get_folders_to_sync(self, path):
        for root, dirs, files in os.walk(path):
            if self.conf.is_blacklisted(root):
                dirs.clear()
                continue
            if self.is_for_sync(root):
                yield root
