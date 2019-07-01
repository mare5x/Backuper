from backuper import settings

settings.SETTINGS_FILE = "tests/test_settings.ini"
settings.DATA_FILE = "tests/test_backuper.ini"

conf = settings.Settings()

def print_ini(c):
    for section in c.sections():
        for item in c.items(section):
            print(item)

def print_settings_file():
    print(settings.SETTINGS_FILE)
    print_ini(conf.user_settings_file)

def print_data_file():
    print(settings.DATA_FILE)
    print_ini(conf.data_file)

def print_settings():
    print_settings_file()
    print_data_file()

if __name__ == '__main__':
    print_settings()
    conf.exit()