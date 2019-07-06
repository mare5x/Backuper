import os
import configparser
import datetime
import logging

from . import googledrive
from . import database as db

from pytools import filetools as ft


ENCODING = "UTF-8"
SETTINGS_FILE = "settings.ini"
DATA_FILE = "backuper.ini"


class BaseFile(configparser.ConfigParser):
    def __init__(self):
        super().__init__()

    def get_values(self, section, option, sep=";"):
        return self.get(section, option).strip(sep).split(sep)

    def get_unified_paths(self, section, option, sep=";"):
        return { db.unify_path(path) 
            for path in self.get_values(section, option, sep=sep) if path }

    def get_unified_values(self, section, option, sep=";"):
        return { db.unify_str(val) 
            for val in self.get_values(section, option, sep=sep) if val }

    def write_to_file(self, path):
        with open(path, 'w', encoding=ENCODING) as f:
            self.write(f)


class UserSettingsFile(BaseFile):
    """Stores user modifiable settings. READ-ONLY. """

    def __init__(self):
        super().__init__()

        if not self.read(SETTINGS_FILE, encoding=ENCODING):
            self.make_layout()
            self.write_to_file(SETTINGS_FILE)

    def make_layout(self):
        self['Paths'] = {
            'log_paths_full': '',
            'log_dirs_only': '',
            'sync_dirs': '',
            'blacklisted': '',
            'default_download_path': ''
        }

        self['Settings'] = {
            'blacklisted_extensions': '',
            'blacklisted_names': ''
        }

    def get_paths_in_option(self, option):
        return self.get_unified_paths("Paths", option)


class DataFile(BaseFile):
    """Stores application specific information."""
    
    def __init__(self):
        super().__init__()

        if not self.read(DATA_FILE, encoding=ENCODING):
            self.make_layout()
            self.write_to_file(DATA_FILE)

    def make_layout(self):
        self['Backuper'] = {
            'blacklisted_paths': ''
        }

        self['GoogleDrive'] = {
            'folder_id': '',
            'last_backup_date': '',
            'last_change_token': '',
            'last_download_change_token': '',
            'last_download_sync_time': ''
        }

        self.set_last_download_sync_time()

    def get_last_download_change_token(self):
        v = self["GoogleDrive"]["last_download_change_token"]
        return int(v) if v else -1

    def set_last_download_change_token(self, change_id):
        self['GoogleDrive']['last_download_change_token'] = str(change_id)

    def set_last_download_sync_time(self, sync_time=None):
        if sync_time is None:
            sync_time = googledrive.convert_datetime_to_google_time(datetime.datetime.utcnow())
        self['GoogleDrive']['last_download_sync_time'] = sync_time

    def get_last_download_sync_time(self, raw):
        raw_time = self['GoogleDrive']['last_download_sync_time']
        if not raw:
            return googledrive.convert_google_time_to_datetime(raw_time)
        return raw_time

    def set_last_backup_date(self):
        self['GoogleDrive']['last_backup_date'] = googledrive.convert_datetime_to_google_time(datetime.datetime.utcnow())

    def get_last_backup_date(self, archive=False):
        if archive:
            return googledrive.convert_google_time_to_datetime(self['GoogleDrive']['last_backup_date'])
        return self['GoogleDrive']['last_backup_date']

    def get_last_change_token(self):
        v = self["GoogleDrive"]["last_change_token"]
        return int(v) if v else -1

    def set_last_change_token(self, change_id):
        self['GoogleDrive']['last_change_token'] = str(change_id)

    def set_blacklisted_paths(self, blacklisted_paths):
        self['Backuper']['blacklisted_paths'] = ";".join(blacklisted_paths)

    def get_root_folder_id(self):
        return self.get("GoogleDrive", "folder_id", fallback=None)

    def set_root_folder_id(self, val):
        self["GoogleDrive"]["folder_id"] = str(val)


class Settings:
    def __init__(self):
        self.user_settings_file = UserSettingsFile()
        self.data_file = DataFile()

        # blacklisted paths, folder names and file extensions are excluded and so are all
        # the children of those paths/folders
        # blacklisted_extensions work for both folders and files
        self.blacklisted_paths = self.data_file.get_unified_paths("Backuper", "blacklisted_paths")
        self.blacklisted_extensions = self.user_settings_file.get_unified_values('Settings', 'blacklisted_extensions')
        self.blacklisted_names = self.user_settings_file.get_unified_values('Settings', 'blacklisted_names')
        user_blacklist = self.user_settings_file.get_paths_in_option("blacklisted")
        self.blacklisted_paths.update(user_blacklist)

        self.sync_dirs = self.user_settings_file.get_paths_in_option("sync_dirs")

    def exit(self):
        self.data_file.write_to_file(DATA_FILE)

    def contains_blacklisted_ext(self, basename):
        for ext in self.blacklisted_extensions:
            if basename.endswith(ext):
                return True
        return False

    def contains_blacklisted_rules(self, path):
        entry = db.unify_path(path)
        basename = os.path.basename(entry)
        return (basename in self.blacklisted_names) or (self.contains_blacklisted_ext(entry))

    def contains_blacklisted_rules_parent(self, path, stop):
        if path in stop:
            return False
        if self.contains_blacklisted_rules(path):
            return True
        parent = ft.parent_dir(path)
        if parent == path:
            return False
        return self.contains_blacklisted_rules_parent(parent, stop)

    def is_blacklisted(self, path):
        entry = db.unify_path(path)
        if entry in self.blacklisted_paths:
            return True
        return self.contains_blacklisted_rules(entry)

    def is_blacklisted_parent(self, path, stop):
        """ Check if path or parents of path up to stop are blacklisted. 
            stop should be a list of paths or a string
        """
        if path in stop:
            return False
        if self.is_blacklisted(path):
            return True
        parent = ft.parent_dir(path)
        if parent == path:
            return False
        return self.is_blacklisted_parent(parent, stop)

    def blacklist_path(self, entry):
        if not os.path.exists(entry):
            return
        if not self.is_blacklisted_parent(entry, self.sync_dirs):
            self.blacklisted_paths.add(entry)
            logging.info("Added {} to blacklist.".format(entry))
     
    def clean_blacklisted_paths(self):
        """Cleans the saved blacklisted_paths, so that only the most common valid paths remain."""
        new_blacklisted_paths = set()
        for entry in self.blacklisted_paths:
            if os.path.exists(entry) and not self.is_blacklisted_parent(ft.parent_dir(entry), self.sync_dirs):
                new_blacklisted_paths.add(entry)
        self.blacklisted_paths = new_blacklisted_paths
        self.data_file.set_blacklisted_paths(self.blacklisted_paths)
