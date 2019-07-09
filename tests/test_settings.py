from backuper import settings

SETTINGS_FILE = "tests/test_settings.ini"
DATA_FILE = "tests/test_backuper.ini"

conf = settings.Settings(SETTINGS_FILE, DATA_FILE)

def print_unified_paths(c, section, option):
    print("[{}]/[{}]".format(section, option))
    for path in c.get_unified_paths(section, option):
        print(path)

def print_ini(c):
    for section in c.sections():
        print("[{}]".format(section))
        for item in c.items(section):
            print(item)

def print_settings_file():
    print(conf.user_settings_file.file_path)
    print_ini(conf.user_settings_file)

def print_data_file():
    print(conf.data_file.file_path)
    print_ini(conf.data_file)

def print_settings():
    print_settings_file()
    print_data_file()

if __name__ == '__main__':
    print_settings()
    print_unified_paths(conf.user_settings_file, "Paths", "tree_with_files")
    print_unified_paths(conf.user_settings_file, "Paths", "sync_dirs")
    conf.exit()