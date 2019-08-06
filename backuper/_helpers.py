"""
Various maintenance tools.
"""

import os
import concurrent.futures
import logging
import time

from backuper import database, settings
from pytools import progressbar


def get_unarchived_files_in_google_drive(google, folder_id):
    db = database.GoogleDriveDB()
    
    for dirpath, dirnames, filenames in google.walk_folder(folder_id, fields="files(id,name)"):
        file_id = dirpath[1]
        archive = db.get("drive_id", file_id)
        if archive is None: yield file_id

        for resp in filenames:
            file_id = resp["id"]
            archive = db.get("drive_id", file_id)
            if archive is None: yield file_id

    db.close()

def delete_unarchived_files_in_google_drive(google, folder_id):
    ids = list(get_unarchived_files_in_google_drive(google, folder_id))
    google.batch_delete(ids)

def remove_gd_nonexistent_from_db(google, config):
    """Rebuild database by removing non-existent files in Google Drive from the database archive.

    Used for maintenance.
    """
    print("Removing non-existent files in Google Drive from the database ...")
    logging.info("remove_gd_nonexistent_from_db()")

    with database.GoogleDriveDB() as db:
        for archive in db.model.select().iterator():
            if google.exists(archive.drive_id): continue
            if not os.path.exists(archive.path) or config.is_blacklisted(archive.path):
                logging.info("Removed {} from database.".format(archive.path))
                archive.delete_instance()

def get_all_removed_from_local_db():
    with database.GoogleDriveDB() as db:
        for archive in db:
            if not os.path.exists(archive.path):
                yield archive

def delete_all_removed_from_local_db_batched(google):
    """:WARNING: Delete files removed from disk from Google Drive and the database."""

    print("\rDeleting files removed from disk from Google Drive ...")    
    
    RETRY_LIMIT = 5

    db = database.GoogleDriveDB()

    ids = { rem.drive_id for rem in get_all_removed_from_local_db() }
    retry_ids = set()
    retry_count = 0
    pbar = progressbar.progressbar(total=len(ids))

    def _batch_delete_callback(file_id, _, exception):
        nonlocal retry_count
        if exception is not None and exception.resp.status != 404:
            if exception.resp.status == 403:  # Rate limit exceeded (probably).
                if retry_count >= RETRY_LIMIT:
                    raise exception
                logging.warning("RETRYING:" + repr(exception))
                retry_ids.add(file_id)
                time.sleep(2**retry_count)
                retry_count += 1
            else:
                raise exception
        else:
            if exception is not None and exception.resp.status == 404:  # File does not exist.
                logging.warning("IGNORING: " + repr(exception))
            retry_count = 0
            archive = db.get("drive_id", file_id)
            pbar.update()
            logging.info("Removed {} ({}) from database and/or Google Drive.".format(archive.drive_id, archive.path))
            archive.delete_instance()
    
    google.batch_delete(ids, callback=_batch_delete_callback)
    while len(retry_ids) > 0:
        ids = set(retry_ids)
        retry_ids.clear()
        google.batch_delete(ids, callback=_batch_delete_callback)
    db.close()

def delete_all_removed_from_local_db(google):
    """:WARNING: Delete files removed from disk from Google Drive and the database."""

    print("Deleting files removed from disk from Google Drive ...")
    
    # 403 errors are the enemy. There is nothing that can be done.
    # The official recommended strategy, exponential backoff doesn't work.
    # Requests must be intentionally throtteled to avoid getting stuck in a 403 loop.
    # For that reason, this function is single threaded and unbatched.

    db = database.GoogleDriveDB()
    archives = list(get_all_removed_from_local_db())
    for archive in progressbar.progressbar(archives):
        google.delete(archive.drive_id)
        logging.info("Removed {} ({}) from database and/or Google Drive.".format(archive.drive_id, archive.path))
        archive.delete_instance()
    db.close()

def get_blacklisted_archives():
    SETTINGS_FILE = "_settings.ini"
    DATA_FILE = "_backuper.ini"
    conf = settings.Settings(SETTINGS_FILE, DATA_FILE)
    with database.GoogleDriveDB() as db:
        for archive in db.model.select().iterator():
            if conf.is_blacklisted_parent(archive.path, conf.sync_dirs):
                yield archive

def remove_blacklisted_paths(google):
    """Removes archived blacklisted paths from Google Drive and the database."""
    
    print("Deleting blacklisted files from Google Drive ...")
    
    db = database.GoogleDriveDB()
    archives = list(get_blacklisted_archives())
    for archive in progressbar.progressbar(archives):
        google.delete(archive.drive_id)
        logging.info("Removed {} ({}) from database and/or Google Drive.".format(archive.drive_id, archive.path))
        archive.delete_instance()
    db.close()

def delete_nonlocal_in_gd(google, folder_id, dry_run=False):
    """Removes all files in the given remote folder that don't exist locally. SLOW. """

    print("Deleting nonlocal files from Google Drive ({}) ...".format(folder_id) + (" (dry)" if dry_run else ""))

    db = database.GoogleDriveDB()

    # Because the remote folder can contain files not in the database,
    # we have no choice but to walk through it.
    removed_archived = []
    removed_unarchived = []
    for dirpath, dirnames, filenames in google.walk_folder(folder_id):
        remote_path, file_id = dirpath
        # print(remote_path)
        archive = db.get("drive_id", file_id)
        if archive:
            if not os.path.exists(archive.path):
                removed_archived.append(archive)
                dirnames.clear()
                continue
        else:
            # A file that exists remotely but isn't in the database
            # is definitely safe to remove.
            # If a file exists with the same path as a local file, 
            # it should still get removed because the local file will
            # get uploaded and put in the database.
            removed_unarchived.append((file_id, remote_path))
            dirnames.clear()
            continue

        for resp in filenames:
            file_id = resp["id"]
            archive = db.get("drive_id", file_id)
            if archive:
                if not os.path.exists(archive.path):
                    removed_archived.append(archive)
            else:
                name = resp["name"]
                removed_unarchived.append((file_id, os.path.join(remote_path, name)))

    # Database entries are removed because the files in question exist
    # neither locally nor remotely.
    for archive in removed_archived:
        # If a folder got removed, all children got removed as well.
        if dry_run:
            print(archive.drive_id, archive.path)
        else:
            google.delete(archive.drive_id)
            logging.info("Removed {} ({}) from database and Google Drive.".format(archive.drive_id, archive.path))
            q = db.model.delete().where(db.model.path.startswith(archive.path))
            q.execute()
    for file_id, remote_path in removed_unarchived:
        if dry_run:
            print(file_id, remote_path)
        else:
            google.delete(file_id)  # logging.info in google.delete

    db.close()

def delete_removed_from_local_db(google, local_path, dry_run=False):
    print("Deleting files removed from database from Google Drive ({}) ...".format(local_path) + (" (dry)" if dry_run else ""))

    db = database.GoogleDriveDB()
    
    archives = []
    q = db.model.select().where(db.model.path.startswith(local_path))
    for archive in q.namedtuples().iterator():
        if not os.path.exists(archive.path):
            archives.append(archive)
    
    removed = set()
    for archive in archives:
        # Minimizing GD API calls is key for speed.
        if dry_run or (archive.drive_id in removed): continue

        print(archive.path, archive.drive_id)
        google.delete(archive.drive_id)
        # If this is a folder, then all the children will get removed as well.
        q = db.model.select().where(db.model.path.startswith(archive.path))
        for arch in q.iterator():
            removed.add(arch.drive_id)
            arch.delete_instance()
            logging.info("Removed {} ({}) from database and Google Drive.".format(archive.drive_id, archive.path))

    db.close()


if __name__ == "__main__":
    from backuper import googledrive
    g = googledrive.GoogleDrive()
    # for _ in get_unarchived_files_in_google_drive(g, "0B94xod46LwqkZENtNWhLMXZ4UzA"): pass
    
    # for archive in get_all_removed_from_local_db():
    #     print(archive.path, archive.drive_id)
    logging.basicConfig(filename='logs/_helpers.log', level=logging.INFO)
    # delete_all_removed_from_local_db_batched(g)
    # delete_all_removed_from_local_db(g)

    # for archive in get_blacklisted_archives():
    #     print(archive.path, archive.drive_id)
    remove_blacklisted_paths(g)

