import os
from backuper import uploader, settings, database, googledrive, filecrawler

SETTINGS_FILE = "tests/test_settings.ini"
DATA_FILE = "tests/test_backuper.ini"


def make_folder_structure(path, drive_uploader, file_crawler):
    path_folder_id = drive_uploader.create_dir(path)
    print(path, path_folder_id)
    for folder_path in file_crawler.get_folders_to_sync(path):
        print(folder_path, drive_uploader.create_dir(folder_path))
    return path_folder_id

def db_upload_test(path):
    db = database.GoogleDriveDB()
    conf = settings.Settings(SETTINGS_FILE, DATA_FILE)
    google = googledrive.GoogleDrive()
    file_crawler = filecrawler.LocalFileCrawler(conf)

    drive_uploader = uploader.DBDriveUploader(conf, google, update_db=True)
    folder_id = make_folder_structure(path, drive_uploader, file_crawler)

    q = drive_uploader.start_upload_queue(n_threads=4)
    for fpath in file_crawler.get_files_to_sync(path):
        q.put(drive_uploader.DUQEntry(fpath))
    drive_uploader.wait_for_queue(q)

    input("Press any key to clean up.")
    google.delete(folder_id)
    entry = database.unify_path(path)
    query = database.DriveArchive.select().where(database.DriveArchive.path.contains(entry))
    for archive in query.iterator():
        archive.delete_instance()

    conf.exit()
    db.close()

def upload_test(path):
    conf = settings.Settings(SETTINGS_FILE, DATA_FILE)
    google = googledrive.GoogleDrive()
    ul = uploader.DriveUploader(conf, google)

    folder_id = ul.create_dir(path, folder_name="BackuperUploadTest", parent_folder_id="root")
    q = ul.start_upload_queue()
    for name in os.listdir(path):
        q.put(ul.DUQEntry(os.path.join(path, name), folder_id))
    ul.wait_for_queue(q)

    input("Press any key to clean up.")
    google.delete(folder_id)

    conf.exit()

if __name__ == "__main__":
    db_upload_test("tests/")
    upload_test("tests/")