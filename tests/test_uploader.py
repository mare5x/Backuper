from backuper import uploader, settings, database, googledrive, filecrawler

settings.SETTINGS_FILE = "tests/test_settings.ini"
settings.DATA_FILE = "tests/test_backuper.ini"


conf = settings.Settings()
database.GoogleDriveDB.init()
google = googledrive.GoogleDrive()
drive_uploader = uploader.DriveUploader(conf, google)
file_crawler = filecrawler.LocalFileCrawler(conf)


def make_folder_structure(path):
    path_folder_id = drive_uploader.create_dir(path)
    print(path, path_folder_id)
    for folder_path in file_crawler.get_folders_to_sync(path):
        print(folder_path, drive_uploader.create_dir(folder_path))
    return path_folder_id

def upload_test(path):
    folder_id = make_folder_structure(path)

    q = drive_uploader.start_upload_queue(n_threads=4)
    for fpath in file_crawler.get_files_to_sync(path):
        q.put(fpath)
    drive_uploader.wait_for_queue(q)

    input("Press any key to clean up.")
    google.delete(folder_id)
    entry = database.unify_path(path)
    query = database.DriveArchive.select().where(database.DriveArchive.path.contains(entry))
    for archive in query.iterator():
        archive.delete_instance()

upload_test("tests/")

database.GoogleDriveDB.close()
conf.exit()