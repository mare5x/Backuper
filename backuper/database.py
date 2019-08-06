import os
import logging
import concurrent.futures

from pytools import filetools as ft

import peewee


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

    FOLDER_MD5 = ""  # Shared md5 checksum for folders.
    
    model = DriveArchive

    def __init__(self):
        GoogleDriveDB.init()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        GoogleDriveDB.close()

    def __iter__(self):
        return GoogleDriveDB.model.select().iterator()

    @staticmethod
    def init():
        db.connect(reuse_if_open=True)
        db.create_tables([GoogleDriveDB.model], safe=True)

    @staticmethod
    def close():
        db.close()

    @staticmethod
    def get(field, key, fallback=None):
        query = getattr(GoogleDriveDB.model, field) == key
        try:
            return GoogleDriveDB.model.get(query)
        except GoogleDriveDB.model.DoesNotExist:
            return fallback

    @staticmethod
    def create(*args, **kwargs):
        with db.atomic():
            return GoogleDriveDB.model.create(*args, **kwargs)

    @staticmethod
    def remove(field, key):
        inst = GoogleDriveDB.get(field, key)
        if inst is not None:
            inst.delete_instance()

    @staticmethod
    def update(inst, **kwargs):
        for key, value in kwargs.items():
            setattr(inst, key, value)
        return inst.save()

    @staticmethod
    def create_or_update(**kwargs):
        # Use unique fields for the 'get' part. And the rest
        # for updating/creating.
        UNIQUE = ["path", "drive_id"]
        get = dict()
        for field in UNIQUE:
            if field in kwargs:
                get[field] = kwargs.pop(field)

        model, created = GoogleDriveDB.model.get_or_create(**get, defaults=kwargs)
        if not created:
            GoogleDriveDB.update(model, **kwargs)
        return model

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
    print("Replacing database entries {} => {} ...".format(old_path, new_path))
    logging.info("rename_database_path({}, {})".format(old_path, new_path))
    
    old_path = unify_path(old_path)
    new_path = unify_path(new_path)
    
    with GoogleDriveDB() as db:
        q = db.model.select().where(db.model.path.startswith(old_path))
        for archive in q.iterator():
            db.update(archive, path=archive.path.replace(old_path, new_path, count=1))
