import os
import logging
import concurrent.futures

import peewee
from tqdm import tqdm

db = peewee.SqliteDatabase('archived.db')

class BaseModel(peewee.Model):
    path = peewee.TextField(unique=True)

    class Meta:
        database = db


class DriveArchive(BaseModel):
    drive_id = peewee.CharField(unique=True)
    date_modified_on_disk = peewee.DateTimeField()
    md5sum = peewee.CharField(null=True)
    

def db_get(model, field, key, fallback=None):
    try:
        return model.get(field == key)
    except model.DoesNotExist:
        return fallback

def db_create(model, *args, **kwargs):
    with db.atomic():
        return model.create(*args, **kwargs)

def db_update(model_instance, **kwargs):
    for key, value in kwargs.items():
        setattr(model_instance, key, value)
    return model_instance.save()

def db_create_or_update(model, **kwargs):
    model, created = model.get_or_create(**kwargs)
    if not created:
        db_update(model, **kwargs)

def db_init():
    db.connect()
    db.create_tables([DriveArchive], safe=True)

def db_exit():
    db.close()


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