import os
import logging
import concurrent.futures

from pytools import filetools as ft

import peewee
from tqdm import tqdm


DB_FILE_PATH = "archived.db"
db = peewee.SqliteDatabase(DB_FILE_PATH)


class BaseModel(peewee.Model):
    path = peewee.TextField(unique=True)

    class Meta:
        database = db


class DriveArchive(BaseModel):
    drive_id = peewee.CharField(unique=True)
    date_modified_on_disk = peewee.DateTimeField()
    md5sum = peewee.CharField(null=True)


class GoogleDriveDB:
    """Manages the archive (database) used for Google Drive."""

    def __init__(self):
        GoogleDriveDB.init()

    @staticmethod
    def init():
        db.connect(reuse_if_open=True)
        db.create_tables([DriveArchive], safe=True)

    @staticmethod
    def close():
        db.close()

    @staticmethod
    def get(field, key, fallback=None):
        query = getattr(DriveArchive, field) == key
        try:
            return DriveArchive.get(query)
        except DriveArchive.DoesNotExist:
            return fallback

    @staticmethod
    def create(*args, **kwargs):
        with db.atomic():
            return DriveArchive.create(*args, **kwargs)

    @staticmethod
    def remove(field, key):
        inst = GoogleDriveDB.get(field, key)
        if inst is not None:
            inst.delete_instance()

    @staticmethod
    def update(**kwargs):
        for key, value in kwargs.items():
            setattr(DriveArchive, key, value)
        return DriveArchive.save()

    @staticmethod
    def create_or_update(**kwargs):
        model, created = DriveArchive.get_or_create(**kwargs)
        if not created:
            GoogleDriveDB.update(**kwargs)

    @staticmethod
    def get_parent_folder_id(path, fallback="root"):
        return GoogleDriveDB.get_stored_path_id(ft.parent_dir(path), fallback=fallback)

    @staticmethod
    def get_stored_path_id(path, fallback=None):
        val = GoogleDriveDB.get("path", path, fallback=None)
        if val: return val.drive_id
        return fallback


def unify_path(path):
    """All paths stored in the database must go through this function!"""
    return os.path.normcase(os.path.abspath(path))

def unify_str(txt):
    return os.path.normcase(txt)


def rename_database_path(old_path, new_path):
    """Replace all database paths that contain old_path to contain new_path.
    
    Use it when moving a folder to a different location on your drive.
    """
    print("Replacing {} database entries to {} ...".format(old_path, new_path))
    logging.info("rename_database_path({}, {})".format(old_path, new_path))
    
    old_path = unify_path(old_path)
    new_path = unify_path(new_path)
    
    q = DriveArchive.select().where(DriveArchive.path.startswith(old_path))
    with tqdm(total=q.count()) as pbar:
        for archive in q.iterator():
            db_update(archive, path=archive.path.replace(old_path, new_path, count=1))
            pbar.update()

def clean_database():
    """Remove every locally non-existent file from the database.
    
    Use with caution.
    """
    pass

def rebuild_database(google, config):
    """Rebuild database by removing non-existent files in Google Drive from the database archive.

    Used for maintenance.
    """
    print("Rebuilding database ...")
    logging.info("rebuild_database()")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor, \
                    tqdm(total=DriveArchive.select().count()) as pbar:
        futures = {}
        for archive in DriveArchive.select().iterator():
            futures[executor.submit(retry_operation, self.google.exists, archive.drive_id, error=RETRYABLE_ERRORS)] = archive

        for future in concurrent.futures.as_completed(futures):
            pbar.update()

            if not future.result():  # doesn't exist
                archive = futures[future]
                if not os.path.exists(archive.path) or self.is_blacklisted(archive.path):
                    logging.info("Removed {} from database.".format(archive.path))
                    archive.delete_instance()
            del futures[future]