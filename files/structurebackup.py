#! python3

from collections import namedtuple
import shelve
import configparser
import tempfile
import datetime
from files.fileutils import *

import dropbox
import httplib2
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.client import OAuth2WebServerFlow


def uploading_to(loc):
    def wrap(func):
        def print_info(*args, **kwargs):
            print("Uploading {2} ({1}) to {0}".format(loc, get_file_size(*args[1:]), *args[1:]))
            return func(*args, **kwargs)
        return print_info
    return wrap


def get_shelf(key, fallback=None):
    try:
        with shelve.open('settings') as db:
            return db[key]
    except KeyError:
        return fallback


def set_shelf(key, item):
    with shelve.open('settings') as db:
        db[key] = item


def clear_shelf(blacklist=None):
    with shelve.open('settings') as db:
        for item in db:
            if item not in blacklist:
                db.pop(item)


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
            'dirs_to_archive': ''
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
            'folder_id': ''
        }

    def write_to_config(self):
        with open('settings.ini', 'w') as configfile:
            self.write(configfile)


class Backup():
    def __init__(self, save_to_path=".", clean=False, my_dropbox=None, my_google=None):
        self.config = Config()
        self.temp_dir_path = save_to_path
        self.clean = clean
        self.my_dropbox = my_dropbox
        self.my_google = my_google

        self.Path = namedtuple('Path', ['id', 'modified_date'])
        self.drive_archived = get_shelf('drive_archived', {})

        self.drive_archived_files = get_shelf('drive_archived_files', {})
        self.drive_archived_dirs = get_shelf('drive_archived_dirs', {})

    def __enter__(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = self.temp_dir.name
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        path_to_zip = zip_dir(self.temp_dir_path)
        if self.my_google:
            self.to_google_drive(self.temp_dir_path)
        if self.my_dropbox:
            self.to_dropbox(path_to_zip)
        if self.clean:
            remove_file(path_to_zip)

        self._save_archive()
        print("\nDONE")

    def to_dropbox(self, path):
        self.my_dropbox.upload_file(path, file_name="{}.zip".format(get_date(for_file=True)))

    def to_google_drive(self, path):
        root_folder_id = self._get_drive_root_folder_id()
        if os.path.isdir(path):
            # root_id = root_folder_id
            for root, dirs, files in scandir.walk(path):
                # depth = root.count(os.sep) - path.count(os.sep)
                dir_id = self._dir_to_drive(root)
                for _file in files:
                    self._file_to_drive(os.path.join(root, _file), dir_id)

                # if modified_date(temp_path) > modified_date() or path not in self.google_files:
        else:
            self._file_to_drive(path, root_folder_id)

        self._save_archive()

    def _save_archive(self):
        set_shelf('drive_archived_files', self.drive_archived_files)
        set_shelf('drive_archived_dirs', self.drive_archived_dirs)

    def _dir_to_drive(self, path):
        parent_id = self.drive_archived_dirs.get(parent_dir(path), self._get_drive_root_folder_id())

        entry = os.path.abspath(path)
        try:
            new_id = self.drive_archived_dirs[entry]
        except KeyError:
            new_id = self.my_google.create_folder(name_from_path(entry, raw=True), parent_id=parent_id)
            self.drive_archived_dirs[entry] = new_id
        return new_id

    def _file_to_drive(self, path, folder_id):
        file_entry = os.path.abspath(path)
        file_id = self.drive_archived_files.get(file_entry)
        resp = self.my_google.upload_file(path, folder_id=folder_id, file_id=file_id)
        self.drive_archived_files[file_entry] = resp['id']
        return resp['id']

    def _get_drive_root_folder_id(self):
        try:
            return self.config['GoogleDrive']['folder_id']
        except KeyError:
            if not self.my_google.get_file_data_by_name("Backuper"):
                folder_id = self.my_google.create_folder("Backuper")
                self.config['GoogleDrive']['folder_id'] = folder_id
            else:
                folder_id = self.my_google.get_file_data_by_name("Backuper")['id']

        return folder_id

    def get_paths_to_backup(self):
        # archived_files = {}
        # result = []
        # for section in self.config['Paths']:
        #     t_result = []
        #     for path in section:
        #         path = os.path.realpath(path)
        #         archived_files[path] = date_modified(path, walk=True)
        #         try:
        #             archived_file = get_shelf('archived_files')[path]
        #         except KeyError:
        #             t_result.append(path)
        #             set_shelf('archived_files', archived_files)
        #         else:
        #             if self.newer_date_modified_exists(path, self.my_google.get_modified_date(archived_file)):
        #                 t_result.append(path)

        #     result.append(t_result)

        # return result[0], result[1], result[2]

        paths_to_backup = [os.path.realpath(path) for path in self.config['Paths']['paths_to_backup'].split(';')]
        dir_only_paths = [os.path.realpath(path) for path in self.config['Paths']['dir_only_paths'].split(';')]
        dirs_to_archive = [os.path.realpath(path) for path in self.config['Paths']['dirs_to_archive'].split(';')]
        return paths_to_backup, dir_only_paths, dirs_to_archive

    def newer_date_modified_exists(self, path, date):
        for root, dirs, files in scandir.walk(path):
            if date_modified(join(root, dirs)) > date:
                return True
        return False

    def _clear_shelf(self):
        clear_shelf(['credentials'])

    def write_backup_file(self, save_to=".", path=".", get_dirs_only=False):
        file_name = r"{}\{}".format(save_to, name_from_path(path, ".txt"))
        with open(file_name, "w", encoding="utf8") as f:
            with redirect_stdout(f):
                log_structure(path, dirs_only=get_dirs_only)


class Dropbox():
    def __init__(self, overwrite=False):
        config = Config()
        APPKEY = config["Dropbox"]["appkey"]
        APPSECRET = config["Dropbox"]["appsecret"]
        ACCESSTOKEN = config["Dropbox"]["accesstoken"]
        try:
            self.client = dropbox.client.DropboxClient(ACCESSTOKEN)
        except ValueError as e:
            print(e, "\nFill in settings.ini")
        self.overwrite = overwrite

    def get_latest_file_metadata(self):
        last_modified = datetime.datetime(1900, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        metadata = None
        for item in self.client.metadata("/")["contents"]:
            _time = datetime.datetime.strptime(item['modified'], "%a, %d %b %Y %H:%M:%S %z")
            if _time > last_modified:
                last_modified = _time
                metadata = item

        return metadata

    def get_rev(self):
        return self.get_latest_file_metadata()["rev"]

    def get_file_name(self):
        return name_from_path(self.get_latest_file_metadata()['path'], raw=True)

    @uploading_to('Dropbox')
    def upload_file(self, file_path, file_name=None):
        with open(file_path, "rb") as f:
            file_size = getsize(file_path)
            uploader = DropboxUploader(self.client, f, file_size)

            while True:
                try:
                    uploader.upload_chunked()
                    break
                except dropbox.exceptions.MaxRetryError as e:
                    print("connection error, ", e, " retrying")
                    time.sleep(1)

            if self.overwrite:
                old_file = self.get_file_name()
                uploader.finish(old_file, parent_rev=self.get_rev())
                if file_name:
                    self.client.file_move(old_file, "{}".format(file_name))
                else:
                    self.client.file_move(old_file, name_from_path(file_path, raw=True))
            else:
                if file_name:
                    uploader.finish("{}".format(file_name))
                else:
                    uploader.finish(name_from_path(file_path, raw=True))


class DropboxUploader(dropbox.client.ChunkedUploader):
    """Python3 compatibility"""

    def __init__(self, *args):
        super().__init__(*args)
        self.time_started = time.time()

    def progress_bar(self):
        uploaded = (self.offset + 1) / self.target_length  # avoid 0 division error
        time_left = ((time.time() - self.time_started) / uploaded) - (time.time() - self.time_started)
        return "{:.2f}% uploaded [elapsed: {}, left: {}]".format(100 * uploaded,
                                                                 get_time_from_secs(time.time() - self.time_started),
                                                                 get_time_from_secs(time_left))

    def upload_chunked(self, chunk_size=4 * 1024 * 1024):
        """Uploads data from this ChunkedUploader's file_obj in chunks, until
        an error occurs. Throws an exception when an error occurs, and can
        be called again to resume the upload.

        Parameters
            chunk_size
              The number of bytes to put in each chunk. (Default 4 MB.)
        """

        while self.offset < self.target_length:
            print(self.progress_bar(), end="\r")

            next_chunk_size = min(chunk_size, self.target_length - self.offset)
            if self.last_block is None:
                self.last_block = self.file_obj.read(next_chunk_size)

            try:
                (self.offset, self.upload_id) = self.client.upload_chunk(
                    self.last_block, next_chunk_size, self.offset, self.upload_id)
                self.last_block = None
            except dropbox.rest.ErrorResponse as e:
                # Handle the case where the server tells us our offset is wrong.
                must_reraise = True
                if e.status == 400:
                    reply = e.body
                    if "offset" in reply and reply['offset'] != 0 and reply['offset'] > self.offset:
                        self.last_block = None
                        self.offset = reply['offset']
                        must_reraise = False
                if must_reraise:
                    raise


class GoogleDrive:
    def __init__(self):
        self.config = Config()
        CLIENT_ID = self.config['GoogleDrive']['client_id']
        CLIENT_SECRET = self.config['GoogleDrive']['client_secret']
        OAUTH_SCOPE = self.config['GoogleDrive']['oauth_scope']
        REDIRECT_URI = self.config['GoogleDrive']['redirect_uri']

        credentials = get_shelf('credentials', None)

        flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, redirect_uri=REDIRECT_URI)
        http = httplib2.Http()

        while True:
            try:
                http = credentials.authorize(http)
            except:
                authorize_url = flow.step1_get_authorize_url()
                print(authorize_url)
                code = input("enter: ").strip()
                credentials = flow.step2_exchange(code)
                set_shelf('credentials', credentials)
                continue
            break

        self.drive_service = build('drive', 'v2', http=http)

    def progress_bar(self, status, time_started):
        time_left = ((time.time() - time_started) / status.progress()) - (time.time() - time_started)
        return "{:.2f}% uploaded [elapsed: {}, left: {}]".format(status.progress() * 100,
                                                                 get_time_from_secs(time.time() - time_started),
                                                                 get_time_from_secs(time_left))

    def upload(self, path, folder_id='root', file_id=None):
        if os.path.isdir(path):
            return self.upload_directory(path, root_id=folder_id)
        return self.upload_file(path, folder_id=folder_id, file_id=file_id)

    @uploading_to('Google Drive')
    def upload_file(self, file_path, folder_id='root', file_id=None):
        body = {
            'title': name_from_path(file_path, raw=True),
            'parents': [{'id': folder_id}]
        }

        if getsize(file_path):
            media_body = MediaFileUpload(file_path, chunksize=4 * 1024 ** 2, resumable=True)
        else:
            return self.drive_service.files().insert(body=body).execute()

        if file_id:
            request = self.drive_service.files().update(fileId=file_id, body=body, media_body=media_body)
        else:
            request = self.drive_service.files().insert(body=body, media_body=media_body)

        time_started = time.time()
        response = None
        while response is None:
            status, response = request.next_chunk(num_retries=500)
            if status:
                print(self.progress_bar(status, time_started), end="\r")

        return response

    @uploading_to('Google Drive')
    def upload_directory(self, dir_path, root_id='root'):
        archived_dirs = {}
        for root, dirs, files in scandir.walk(dir_path):
            parent_id = archived_dirs.get(parent_dir(root), root_id)

            try:
                dir_id = archived_dirs[os.path.abspath(root)]
            except KeyError:
                dir_id = self.create_folder(name_from_path(root, raw=True), parent_id=parent_id)
                archived_dirs[os.path.abspath(root)] = dir_id

            for _file in files:
                self.upload_file(os.path.join(root, _file), folder_id=dir_id)

    def create_folder(self, name, parent_id='root'):
        body = {
            'title': name,
            'parents': [{'id': parent_id}],
            'mimeType': 'application/vnd.google-apps.folder'
        }

        return self.drive_service.files().insert(body=body).execute()['id']

    def get_modified_date(self, file_id):
        return datetime.datetime.strptime(self.get_metadata(file_id)['modifiedDate'].rsplit('.', 1)[0],
                                          '%Y-%m-%dT%H:%M:%S')

    def get_metadata(self, file_id):
        return self.drive_service.files().get(fileId=file_id).execute()

    def get_file_data_by_name(self, name):
        for item in self.drive_service.files().list().execute()['items']:
            if name == item['title']:
                return item

# compare old new modified time
# upload and replace (update) only newer file
# 1 folder for all (no more DATE.zip)
#
# file_path: modified_date
# if date_modified is newer than current modified date in cloud

# upload each file seperately, store it's id: modified_date

"""In [27]: g.drive_service.files().get(fileId=g.get_stored_file_id()).execute()['modifiedDate'].rsplit('.', 1)[0]
Out[27]: '2015-06-05T14:59:19'

In [28]: datetime.datetime.strptime('2015-06-05T14:59:19', '%Y-%m-%dT%H:%M:%S')
Out[28]: datetime.datetime(2015, 6, 5, 14, 59, 19)
"""

# path: (id, modified_date)
