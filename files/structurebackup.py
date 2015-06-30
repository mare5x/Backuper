#! python3

from collections import namedtuple
import mimetypes
import configparser
import tempfile
import datetime
import time
from files.fileutils import *

import dropbox
import httplib2
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from apiclient.errors import ResumableUploadError
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage
from oauth2client.tools import run_flow
from oauth2client import tools

from peewee import *


db = SqliteDatabase('archived.db')


class BaseModel(Model):
    path = TextField(unique=True)

    class Meta:
        database = db


class DriveArchive(BaseModel):
    drive_id = CharField(unique=True)
    date_modified_on_disk = DateTimeField()


class DropboxArchive(BaseModel):
    dropbox_id = CharField(unique=True)
    date_modified_on_disk = DateTimeField()


def dynamic_print(s, fit=False):
    if fit and len(str(s)) > term_width():
        s = str(s)[-term_width():]
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
            path = "\\".join(args[1].rsplit('\\', 2)[-2:])
            if dynamic:
                dynamic_print("Uploading {} ({}) to {}".format(path, get_file_size(*args[1:]), loc), True)
            else:
                print("Uploading {} ({}) to {}".format(path, get_file_size(*args[1:]), loc))
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
        break
    return None


def db_get(model, field, key, fallback=None):
    try:
        return model.get(field == key)
    except model.DoesNotExist:
        return fallback


def db_create(model, *args, **kwargs):
    with db.atomic():
        return model.create(*args, **kwargs)


def db_update(model, **kwargs):
    for key, value in kwargs.items():
        setattr(model, key, value)
    return model.save()


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


class Backup:
    def __init__(self, save_to_path=".", clean=False, my_dropbox=None, my_google=None):
        self.config = Config()
        self.temp_dir_path = save_to_path
        self.clean = clean
        self.my_dropbox = my_dropbox
        self.my_google = my_google

        db.connect()
        db.create_tables([DriveArchive, DropboxArchive], True)

    def __enter__(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = self.temp_dir.name
        return self

    def __exit__(self, *exc):
        path_to_zip = zip_dir(self.temp_dir_path)
        if self.my_google:
            self.to_google_drive(path_to_zip)
        if self.my_dropbox:
            self.to_dropbox(path_to_zip)
        if self.clean:
            remove_file(path_to_zip)

        db.close()
        print("\nDONE")

    def to_dropbox(self, path):
        self.my_dropbox.upload_file(path, file_name="{}.zip".format(get_date(for_file=True)))

    def to_google_drive(self, path):
        if os.path.isdir(path):
            for root, dirs, files in scandir.walk(path):
                dir_id = retry_operation(self._dir_to_drive, root, num_retries=60, wait_time=1, error=ResumableUploadError)
                if dir_id is None:
                    print('Skipped {}'.format(root))
                    continue
                for _file in files:
                    if self._is_newer_date(os.path.join(root, _file)):
                        if retry_operation(self._file_to_drive, os.path.join(root, _file), dir_id, num_retries=60,
                                           wait_time=1, error=ResumableUploadError) is None:
                            print('Skipped {}'.format(os.path.join(root, _file)))
                            continue
        else:
            if self._is_newer_date(path):
                retry_operation(self._file_to_drive, path, self._get_drive_root_folder_id(), num_retries=60, wait_time=1,
                                error=ResumableUploadError)

    def _dir_to_drive(self, path):
        try:
            parent_id = DriveArchive.get(DriveArchive.path == parent_dir(path)).drive_id
        except DriveArchive.DoesNotExist:
            parent_id = self._get_drive_root_folder_id()

        # if not self.my_google.exists(parent_id):
        #     DriveArchive.delete_instance(DriveArchive.get(DriveArchive.drive_id == parent_id))

        entry = os.path.abspath(path)
        try:
            new_id = DriveArchive.get(DriveArchive.path == entry).drive_id
        except DriveArchive.DoesNotExist:
            new_id = self.my_google.create_folder(name_from_path(entry, raw=True), parent_id=parent_id)
            db_create(DriveArchive, path=entry, drive_id=new_id, date_modified_on_disk=date_modified(entry))
        return new_id

    def _file_to_drive(self, path, folder_id):
        file_entry = os.path.abspath(path)
        try:
            file_id = DriveArchive.get(DriveArchive.path == file_entry).drive_id
        except DriveArchive.DoesNotExist:
            file_id = None

        resp = self.my_google.upload_file(path, folder_id=folder_id, file_id=file_id)
        try:
            db_create(DriveArchive, path=file_entry, drive_id=resp['id'], date_modified_on_disk=date_modified(file_entry))
        except IntegrityError:
            model = DriveArchive.get(DriveArchive.path == file_entry)
            db_update(model, path=file_entry, drive_id=resp['id'], date_modified_on_disk=date_modified(file_entry))

        return resp['id']

    def _is_newer_date(self, path):
        entry = os.path.abspath(path)
        try:
            modified_date = DriveArchive.get(DriveArchive.path == entry).date_modified_on_disk
            return date_modified(entry) > modified_date
        except DriveArchive.DoesNotExist:
            return True

    def _get_drive_root_folder_id(self):
        try:
            return self.config['GoogleDrive']['folder_id']
        except KeyError:
            if not self.my_google.get_file_data_by_name("Backuper"):
                folder_id = self.my_google.create_folder("Backuper")
            else:
                folder_id = self.my_google.get_file_data_by_name("Backuper")[0]['id']
            self.config['GoogleDrive']['folder_id'] = folder_id

        return folder_id

    def get_paths_to_backup(self):
        all_paths = {}
        for section in self.config['Paths']:
            paths = [os.path.abspath(path) for path in self.config['Paths'][section].split(';')]
            all_paths[section] = paths
        return all_paths

    def clear_redundant_archives(self):
        for archive in DriveArchive.select().where(os.path.exists(DriveArchive.path) == False):
            self.my_google.delete(archive.drive_id)

        return DriveArchive.delete().where(os.path.exists(DriveArchive.path) == False).execute()

    # def _archive_remove_deleted_entries(self):
    #     for path in self.drive_archived:
    #         if not os.path.exists(path):
    #             self.drive_archived.pop(path)

    # def clear_drive_archived(self):
    #     self.drive_archived.clear()

    def delete_from_drive(self, path):
        path = os.path.abspath(path)
        if path in self.drive_archived:
            self.my_google.delete(self.drive_archived[path].id)
            if os.path.isdir(path):
                for root, dirs, files in scandir.walk(path):
                    self.drive_archived.pop(root)
                    for _file in files:
                        self.drive_archived.pop(os.path.join(root, _file))
            else:
                self.drive_archived.pop(path)

    def write_backup_file(self, save_to=".", path=".", get_dirs_only=False):
        file_name = r"{}\{}".format(save_to, name_from_path(path, ".txt"))
        with open(file_name, "w", encoding="utf8") as f:
            with redirect_stdout(f):
                log_structure(path, dirs_only=get_dirs_only)


class Dropbox:
    def __init__(self, overwrite=False):
        config = Config()
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

    @uploading_to('Dropbox', dynamic=True)
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
            dynamic_print(self.progress_bar(), True)

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
    CHUNK_SIZE = 4 * 1024 ** 2
    CREDENTIALS_FILE = 'credentials.json'

    def __init__(self):
        self.config = Config()
        CLIENT_ID = self.config['GoogleDrive']['client_id']
        CLIENT_SECRET = self.config['GoogleDrive']['client_secret']
        OAUTH_SCOPE = self.config['GoogleDrive']['oauth_scope']
        REDIRECT_URI = self.config['GoogleDrive']['redirect_uri']

        flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, redirect_uri=REDIRECT_URI)
        credential_storage = Storage(self.CREDENTIALS_FILE)
        credentials = credential_storage.get()
        if credentials is None or credentials.invalid:
            flags = tools.argparser.parse_args(args=[])
            credentials = run_flow(flow, credential_storage, flags)

        http = credentials.authorize(httplib2.Http())

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

    @uploading_to('Google Drive', dynamic=True)
    def upload_file(self, file_path, folder_id='root', file_id=None):
        mime, encoding = mimetypes.guess_type(file_path)
        if mime is None:
            mime = 'application/octet-stream'

        body = {
            'title': name_from_path(file_path, raw=True),
            'parents': [{'id': folder_id}]
        }

        if getsize(file_path):
            media_body = MediaFileUpload(file_path, mimetype=mime, chunksize=self.CHUNK_SIZE, resumable=True)
        else:
            return self.drive_service.files().insert(body=body).execute()

        request = self._determine_update_or_insert(body, media_body, file_id=file_id)

        time_started = time.time()
        response = None
        while response is None:
            status, response = request.next_chunk(num_retries=500)
            if status:
                dynamic_print(self.progress_bar(status, time_started), True)

        return response

    def _determine_update_or_insert(self, body, media_body, file_id=None):
        if file_id:
            return self.drive_service.files().update(fileId=file_id, body=body, media_body=media_body)
        return self.drive_service.files().insert(body=body, media_body=media_body)

    @uploading_to('Google Drive', dynamic=True)
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
        return self.drive_service.files().list(q="title='{}'".format(name)).execute()['items']

    def delete(self, file_id):
        self.drive_service.files().delete(fileId=file_id).execute()

    def exists(self, file_id):
        if self.get_metadata(file_id) and not self.get_metadata(file_id)['labels']['trashed']:
            return True
        return False


# TODO: uploading show file being uploaded on same line (\r) and progress for whole process not just for individual files
# TODO: multithreaded sync
