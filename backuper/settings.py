import os
import configparser
import datetime
import logging
import re
import fnmatch

from . import googledrive
from . import database as db

from pytools import filetools as ft


ENCODING = "UTF-8"
SEP = ";"


class BaseFile(configparser.ConfigParser):
    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path

    def get(self, *args, **kwargs):
        val = super().get(*args, **kwargs)
        # Overriden because empty strings '' don't trigger
        # the fallback.
        if not val:
            return kwargs.get("fallback", val)
        return val

    def get_values(self, section, option, sep=";"):
        return [val for val in [val.strip() for val in self.get(section, option).strip(sep).split(sep) if val] if val]

    def get_unified_paths(self, section, option, sep=";"):
        # Paths are stripped to allow multiline values.
        return { db.unify_path(path) 
            for path in self.get_values(section, option, sep=sep) }

    def get_unified_values(self, section, option, sep=";"):
        return { db.unify_str(val) 
            for val in self.get_values(section, option, sep=sep) }

    def write_to_file(self):
        with open(self.file_path, 'w', encoding=ENCODING) as f:
            self.write(f)


class UserSettingsFile(BaseFile):
    """Stores user modifiable settings. READ-ONLY. """

    def __init__(self, file_path):
        super().__init__(file_path)

        if not self.read(self.file_path, encoding=ENCODING):
            self.init_file(self.file_path)

    def init_file(self, path):
        import textwrap
        s = """\
        [Settings]
        # Options with multiple values should be seperated using ';'.
        # Seperated values can span multiple lines.
        # E.g.
        # sync_dirs = 
        #   /this/path;
        #   /and/this/path;
        
        # These local directory paths will be synced with /My Drive/Backuper/[...].
        sync_dirs = 
        
        # Download changed untracked files (those not part of a synced directory) (true) or not (false)?
        download_untracked_changes = true

        # Where to download untracked files if 'download_untracked' is true.
        default_download_path = ./downloads/
        
        # These paths (files or directories) won't get synced.
        blacklisted_paths = 

        # Files/directories matching these Unix shell-style wildcards (fnmatch) rules won't get synced.
        blacklisted_rules = 
            *.ipch;
            *.pdb;
            *.ilk;
            *.tlog;
            *.vc.db;
            *.pyc;
            *.lnk;
            Thumbs.db;
            *.vs;
            __pycache__;
            ~$*;

        # Generate a tree log of the given directories, including files.
        tree_with_files = 
        
        # Generate a tree log of the given directories, without files.
        tree_dirs = 
        
        # Keep tree zip?
        tree_keep_local = true
        
        # Where to keep the tree zip if 'tree_keep_local' is true.
        tree_keep_path = ./trees/

        # The number of threads to use when downloading/uploading.
        download_threads = 5
        upload_threads = 5
        """
        s = textwrap.dedent(s)
        # Write a pretty representation to file, because
        # using configparser's methods would remove formatting (and comments).
        with open(path, "w", encoding=ENCODING) as f:
            f.write(s)
        self.read_string(s)  # parse

    def get_paths_in_option(self, option):
        return self.get_unified_paths("Settings", option)

    def get_path_in_option(self, option, fallback="."):
        path = self.get("Settings", option, fallback=fallback).strip(SEP)
        return db.unify_path(path)
    
    def get_bool(self, option):
        return self.getboolean("Settings", option)

    def get_int(self, option):
        return self.getint("Settings", option)

    def get_regex_rules(self, option):
        # fnmatch -> regex patterns -> single compiled regex
        patterns = []
        for rule in self.get_values("Settings", option):
            # On windows case in-sensitive, on unix case sensitive!
            rule = db.unify_str(rule)
            patterns.append(fnmatch.translate(rule))
        pattern = '|'.join(("({})".format(pat) for pat in patterns))
        return re.compile(pattern)


class DataFile(BaseFile):
    """Stores application specific information."""
    
    def __init__(self, file_path):
        super().__init__(file_path)

        if not self.read(self.file_path, encoding=ENCODING):
            self.make_layout()
            self.write_to_file()

    def init_values(self, google):
        token = google.get_start_page_token()
        if self.get_last_removed_change_token() == -1:
            self.set_last_removed_change_token(token)
        if self.get_last_download_change_token() == -1:
            self.set_last_download_change_token(token)

    def make_layout(self):
        self['Backuper'] = {
            'blacklisted_paths': ''
        }

        self['GoogleDrive'] = {
            'folder_id': '',
            'trees_folder_id': '',
            'last_upload_time': '',
            'last_removed_change_token': '',
            'last_download_change_token': '',
            'last_download_sync_time': ''
        }

        self.set_last_download_sync_time()

    def get_last_download_change_token(self):
        v = self["GoogleDrive"]["last_download_change_token"]
        return int(v) if v else -1

    def set_last_download_change_token(self, change_id):
        self['GoogleDrive']['last_download_change_token'] = str(change_id)

    def get_last_removed_change_token(self):
        v = self["GoogleDrive"]["last_removed_change_token"]
        return int(v) if v else -1

    def set_last_removed_change_token(self, change_id):
        self['GoogleDrive']['last_removed_change_token'] = str(change_id)

    def set_last_download_sync_time(self, sync_time=None):
        if sync_time is None:
            sync_time = googledrive.convert_datetime_to_google_time(datetime.datetime.utcnow())
        self['GoogleDrive']['last_download_sync_time'] = sync_time

    def get_last_download_sync_time(self, raw):
        raw_time = self['GoogleDrive']['last_download_sync_time']
        if not raw:
            return googledrive.convert_google_time_to_datetime(raw_time)
        return raw_time

    def set_last_upload_time(self):
        self['GoogleDrive']['last_upload_time'] = googledrive.convert_datetime_to_google_time(datetime.datetime.utcnow())

    def get_last_upload_time(self, archive=False):
        if archive:
            return googledrive.convert_google_time_to_datetime(self['GoogleDrive']['last_upload_time'])
        return self['GoogleDrive']['last_upload_time']

    def set_blacklisted_paths(self, blacklisted_paths):
        self['Backuper']['blacklisted_paths'] = ";".join(blacklisted_paths)

    def get_root_folder_id(self):
        return self.get("GoogleDrive", "folder_id", fallback=None)

    def set_root_folder_id(self, val):
        self["GoogleDrive"]["folder_id"] = val

    def get_trees_folder_id(self):
        return self.get("GoogleDrive", "trees_folder_id", fallback=None)

    def set_trees_folder_id(self, val):
        self['GoogleDrive']['trees_folder_id'] = val


class Settings:
    def __init__(self, user_settings_path, data_file_path):
        self.user_settings_file = UserSettingsFile(user_settings_path)
        self.data_file = DataFile(data_file_path)

        # blacklisted paths, folder names and file extensions are excluded and so are all
        # the children of those paths/folders
        # blacklisted_extensions work for both folders and files
        self.blacklisted_paths = self.data_file.get_unified_paths("Backuper", "blacklisted_paths")
        user_blacklist = self.user_settings_file.get_paths_in_option("blacklisted_paths")
        self.blacklisted_paths.update(user_blacklist)
        self.blacklisted_rules = [self.user_settings_file.get_regex_rules("blacklisted_rules")]

        self.sync_dirs = self.user_settings_file.get_paths_in_option("sync_dirs")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.exit()

    def exit(self):
        self.data_file.write_to_file()

    def get_root_folder_id(self, google):
        folder_id = self.data_file.get_root_folder_id()
        if folder_id is None:
            # Try to use the root Backuper folder if it exists.
            for resp in google.get_folders_in_folder('root', q="name='Backuper'"):
                folder_id = resp["id"]
                print("Using existing folder as root folder: " + google.get_remote_path(folder_id))
                break
            if folder_id is None:
                folder_id = google.create_folder("Backuper")
            self.data_file.set_root_folder_id(folder_id)
        return folder_id

    def contains_blacklisted_rules(self, path):
        name = os.path.basename(path)
        for rule in self.blacklisted_rules:
            if rule.fullmatch(name):
                return True
        return False

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
            logging.info("BLACKLIST ADD: {}.".format(entry))
     
    def clean_blacklisted_paths(self):
        """Cleans the saved blacklisted_paths, so that only the most common valid paths remain."""
        new_blacklisted_paths = set()
        for entry in self.blacklisted_paths:
            if os.path.exists(entry) and not self.is_blacklisted_parent(ft.parent_dir(entry), self.sync_dirs):
                new_blacklisted_paths.add(entry)
        self.blacklisted_paths = new_blacklisted_paths
        self.data_file.set_blacklisted_paths(self.blacklisted_paths)
