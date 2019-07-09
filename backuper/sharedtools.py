import logging
import time
import shutil
import configparser
import hashlib
import glob
import os

from pytools import filetools

NUM_RETRIES = 6


class Config(configparser.ConfigParser):
    def __init__(self):
        super().__init__()

        if not self.read('settings.ini', encoding="utf-8"):
            self.make_layout()
            self.write_to_config()

    def make_layout(self):
        self['Paths'] = {
            'tree_with_files': '',
            'tree_dirs': '',
            'sync_dirs': '',
            'blacklisted': '',
            'default_download_path': ''
        }

        self['Settings'] = {
            'blacklisted_extensions': '',
            'blacklisted_names': ''
        }

        self['Backuper'] = {
            'blacklisted_paths': ''  # automatically added
        }

        self['Dropbox'] = {
            'appkey': '',
            'appsecret': '',
            'accesstoken': ''
        }

        self['GoogleDrive'] = {
            'client_id': '',
            'client_secret': '',
            'oauth_scope': '',
            'redirect_uri': '',
            'folder_id': '',
            'last_backup_date': '',
            'last_change_token': '',
            'last_download_change_token': '',
            'last_download_sync_time': ''
        }

    def get_section_values(self, section, sep=";"):
        return section.strip(sep).split(sep)

    def write_to_config(self):
        with open('settings.ini', 'w', encoding="utf-8") as configfile:
            self.write(configfile)


class ANSI_ESC_CODES:
    ERASE_LINE = "\x1b[2K\r"


def dynamic_print(s, fit=False, log=True):
    if log:
        logging.info(s)
    if fit and len(str(s)) > term_width() - 1:
        s = str(s)[-term_width() + 4:]
    clear_line()
    print(s, end='', flush=True)


def clear_line():
    print(ANSI_ESC_CODES.ERASE_LINE, end='', flush=True)
    # cols = term_width()
    # print('\r' + (' ' * (cols - 1)), end='\r')


def term_width():
    return shutil.get_terminal_size()[0]


def uploading_to(loc, dynamic=False):
    def wrap(func):
        def print_info(*args, **kwargs):
            logging.info(args[1])
            path = "\\".join(args[1].rsplit('\\', 2)[-2:])
            if dynamic:
                dynamic_print("Uploading {} ({}) to {}".format(path, filetools.get_file_size(*args[1:]), loc), True)
            else:
                logging.info("Uploading {} ({}) to {}".format(path, filetools.get_file_size(*args[1:]), loc))
            return func(*args, **kwargs)
        return print_info
    return wrap


def retry_operation(operation, *args, error=None, **kwargs):
    retries = 0
    while retries < NUM_RETRIES or NUM_RETRIES == 0:
        try:
            return operation(*args, **kwargs)
        except error as e:
            retries += 1
            logging.info("ERROR RETRY: {}".format(e))
            dynamic_print('Retries for {}({}, {}): {}'.format(operation.__name__, args, kwargs, retries), True)
            time.sleep(2 ** retries)
            continue
    logging.warning('{}({},{}) Failed'.format(operation.__name__, args, kwargs))
    return None


def handle_progressless_attempt(error, progressless_attempt, suppress=True, retries=5):
    if progressless_attempt >= retries:
        logging.critical('Failed to make progress.')
        if not suppress:
            raise error
        else:
            return True

    sleeptime = 2**progressless_attempt
    # add tqdm
    dynamic_print('Waiting for {}s before retry {}'.format(sleeptime, progressless_attempt))
    time.sleep(sleeptime)
