"""
Various maintenance tools.
"""

import os
import concurrent.futures
import logging
import time

from tqdm import tqdm

from backuper import database, settings


def get_unarchived_files_in_google_drive(google, folder_id):
    db = database.GoogleDriveDB()
    
    for _, remote_path, response in google.walk_folder_builder(folder_id, "", fields="files(id,name)"):
        archive = db.get("drive_id", response['id'])
        if archive: continue

        print(remote_path, response.get('name', "??NAME??"), response['id'])
        yield response['id']

    db.close()

def delete_unarchived_files_in_google_drive(google, folder_id):
    ids = list(get_unarchived_files_in_google_drive(google, folder_id))
    google.batch_delete(ids)

def get_removed_from_local():
    with database.GoogleDriveDB() as db:
        for archive in db:
            if not os.path.exists(archive.path):
                yield archive

def delete_removed_from_local_batched(google):
    """:WARNING: Delete files removed from disk from Google Drive and the database."""

    print("\rDeleting files removed from disk from Google Drive ...")    
    
    RETRY_LIMIT = 5

    db = database.GoogleDriveDB()

    ids = { rem.drive_id for rem in get_removed_from_local() }
    retry_ids = set()
    retry_count = 0
    pbar = tqdm(total=len(ids))

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

def delete_removed_from_local(google):
    """:WARNING: Delete files removed from disk from Google Drive and the database."""

    print("\rDeleting files removed from disk from Google Drive ...")    
    
    # 403 errors are the enemy. There is nothing that can be done.
    # The official recommended strategy, exponential backoff doesn't work.
    # Requests must be intentionally throtteled to avoid getting stuck in a 403 loop.
    # For that reason, this function is single threaded and unbatched.

    db = database.GoogleDriveDB()
    archives = list(get_removed_from_local())
    for archive in tqdm(archives):
        google.delete(archive.drive_id)
        logging.info("Removed {} ({}) from database and/or Google Drive.".format(archive.drive_id, archive.path))
        archive.delete_instance()
    db.close()

def get_blacklisted_archives():
    SETTINGS_FILE = "_settings.ini"
    DATA_FILE = "_backuper.ini"
    conf = settings.Settings(SETTINGS_FILE, DATA_FILE)
    with database.GoogleDriveDB():
        for archive in database.DriveArchive.select().iterator():
            if conf.is_blacklisted_parent(archive.path, conf.sync_dirs):
                yield archive

def remove_blacklisted_paths(google):
    """Removes archived blacklisted paths from Google Drive and the database."""
    
    print("Deleting blacklisted files from Google Drive ...")
    
    db = database.GoogleDriveDB()
    archives = list(get_blacklisted_archives())
    for archive in tqdm(archives):
        google.delete(archive.drive_id)
        logging.info("Removed {} ({}) from database and/or Google Drive.".format(archive.drive_id, archive.path))
        archive.delete_instance()
    db.close()


if __name__ == "__main__":
    from backuper import googledrive
    g = googledrive.GoogleDrive()
    # for _ in get_unarchived_files_in_google_drive(g, "0B94xod46LwqkZENtNWhLMXZ4UzA"): pass
    
    # for archive in get_removed_from_local():
    #     print(archive.path, archive.drive_id)
    logging.basicConfig(filename='logs/_helpers.log', level=logging.INFO)
    # delete_removed_from_local_batched(g)
    # delete_removed_from_local(g)

    # for archive in get_blacklisted_archives():
    #     print(archive.path, archive.drive_id)
    remove_blacklisted_paths(g)

