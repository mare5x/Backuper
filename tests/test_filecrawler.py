import datetime

from backuper import filecrawler
from backuper import settings
from backuper import database
from backuper import googledrive


SETTINGS_FILE = "tests/test_settings.ini"
DATA_FILE = "tests/test_backuper.ini"


def test_localfilecrawler():
    db = database.GoogleDriveDB()
    conf = settings.Settings(SETTINGS_FILE, DATA_FILE)
    crawler = filecrawler.LocalFileCrawler(conf)

    for p in crawler.get_folders_to_sync("tests/"):
        print(p)

    for p in crawler.get_files_to_sync("tests/"):
        print(p)

    for p in crawler.get_all_paths_to_sync("tests/"):
        print(p)

    conf.exit()
    db.close()

def test_drivecrawler_folder(folder_id):
    db = database.GoogleDriveDB()
    conf = settings.Settings(SETTINGS_FILE, DATA_FILE)
    crawler = filecrawler.DriveFileCrawler(conf, googledrive.GoogleDrive())
    
    for obj in crawler.get_ids_to_download_in_folder(folder_id):
        print(obj)

    conf.exit()
    db.close()

def test_drivecrawler_changes():
    db = database.GoogleDriveDB()
    conf = settings.Settings(SETTINGS_FILE, DATA_FILE)
    crawler = filecrawler.DriveFileCrawler(conf, googledrive.GoogleDrive())
    
    change_date = datetime.datetime(2019, 5, 20)
    change_date = googledrive.convert_datetime_to_google_time(change_date)
    conf.data_file.set_last_download_sync_time(change_date)
    conf.data_file.set_last_download_change_token(989626)

    for obj in crawler.get_changes_to_download():
        print(obj)

    conf.exit()
    db.close()

def test_removed_changes():
    conf = settings.Settings(SETTINGS_FILE, DATA_FILE)
    crawler = filecrawler.DriveFileCrawler(conf, googledrive.GoogleDrive())

    for change in crawler.get_last_removed(update_token=False):
        print(change)

    conf.exit()

if __name__ == "__main__":
    # test_localfilecrawler()
    # test_drivecrawler_folder("1fPEdsFWRjHy010hxiZ5Ome-0w70Sn4-v")
    test_drivecrawler_changes()
    # test_removed_changes()