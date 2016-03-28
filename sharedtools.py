import logging
import time
import shutil
import configparser

from pytools.fileutils import *


class Config(configparser.ConfigParser):
    def __init__(self):
        super().__init__()

        if not self.read('settings.ini'):
            self.make_layout()
            self.write_to_config()

    def make_layout(self):
        self['Paths'] = {
            'paths_to_backup': '',
            'dir_only_paths': '',
            'dirs_to_archive': '',
            'blacklisted': ''
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
            'last_change_token': ''
        }

    def get_section_values(self, section, sep=";"):
        return section.split(sep)

    def write_to_config(self):
        with open('settings.ini', 'w') as configfile:
            self.write(configfile)


def dynamic_print(s, fit=False):
    logging.info(s)
    if fit and len(str(s)) > term_width():
        s = str(s)[-term_width() + 4:]
    clear_line()
    print(s, end='\r', flush=True)


def clear_line():
    cols = term_width()
    print('\r' + (' ' * (cols - 1)), end='\r')


def term_width():
    return shutil.get_terminal_size()[0]


def uploading_to(loc, dynamic=False):
    def wrap(func):
        def print_info(*args, **kwargs):
            logging.info(args[1])
            path = "\\".join(args[1].rsplit('\\', 2)[-2:])
            if dynamic:
                dynamic_print("Uploading {} ({}) to {}".format(path, get_file_size(*args[1:]), loc), True)
            else:
                logging.info("Uploading {} ({}) to {}".format(path, get_file_size(*args[1:]), loc))
            return func(*args, **kwargs)
        return print_info
    return wrap


def retry_operation(operation, *args, num_retries=0, error=None, wait_time=0, **kwargs):
    retries = 0
    while retries < num_retries or num_retries == 0:
        try:
            return operation(*args, **kwargs)
        except error:
            retries += 1
            dynamic_print('Retries for {}(): {}'.format(operation.__name__, retries), True)
            time.sleep(wait_time)
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


def unify_path(path):
    return os.path.normcase(os.path.abspath(path))
