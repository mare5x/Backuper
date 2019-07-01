from backuper import filecrawler
from backuper import settings
from backuper import database

database.db_init()

settings.SETTINGS_FILE = "tests/test_settings.ini"
settings.DATA_FILE = "tests/test_backuper.ini"
conf = settings.Settings()

crawler = filecrawler.FileCrawler(conf)

for p in crawler.get_folders_to_sync("tests/"):
    print(p)

for p in crawler.get_files_to_sync("tests/"):
    print(p)

for p in crawler.get_all_paths_to_sync("tests/"):
    print(p)

conf.exit()
database.db_exit()