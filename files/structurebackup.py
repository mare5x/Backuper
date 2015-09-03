#! python3

import mimetypes
import configparser
import tempfile
import datetime
import time
import logging
from contextlib import contextmanager
from pytools.fileutils import *

import dropbox
import httplib2
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from apiclient.errors import ResumableUploadError, HttpError
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
    logging.info(s)
    if fit and len(str(s)) > term_width():
        s = str(s)[-term_width() + 1:]
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
            'folder_id': ''
        }

    def get_section_values(self, section, sep=";"):
        return section.split(sep)

    def write_to_config(self):
        with open('settings.ini', 'w') as configfile:
            self.write(configfile)


class Backup:
    def __init__(self, save_to_path=".", my_dropbox=None, my_google=None, log=False):
        self.config = Config()
        self.my_dropbox = my_dropbox
        self.my_google = my_google

        self.blacklisted = [os.path.abspath(path) for path in self.config.get_section_values(self.config['Paths']['blacklisted'])]

        if log:
            os.makedirs("./Logs/", exist_ok=True)
            log_file = create_filename("./Logs/{}.txt".format(get_date(True)))
            self.blacklisted.append(log_file)
            logging.basicConfig(filename=log_file,
                                filemode='w',
                                format="%(levelname)s:%(asctime)s: %(message)s",
                                datefmt="%Y-%b-%d, %a %H:%M:%S",
                                level=logging.INFO)
            console = logging.StreamHandler()
            formatter = logging.Formatter("%(message)s")
            console.setFormatter(formatter)
            logging.getLogger('structurebackup').addHandler(console)

        db.connect()
        db.create_tables([DriveArchive, DropboxArchive], True)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        db.close()
        self.config.write_to_config()

    def to_dropbox(self, path):
        self.my_dropbox.upload_file(path, file_name="{}.zip".format(get_date(for_file=True)))

    def to_google_drive(self, path):
        if os.path.isdir(path):
            for _path in self.get_paths_to_sync(path):
                if os.path.isdir(_path):
                    retry_operation(self.dir_to_drive, _path, num_retries=20, wait_time=1, error=ResumableUploadError)
                else:
                    retry_operation(self.file_to_drive, _path, num_retries=20, wait_time=1, error=ResumableUploadError)
        elif self.is_for_sync(path):
            retry_operation(self.file_to_drive, path, self.get_drive_root_folder_id(), num_retries=20, wait_time=1,
                            error=ResumableUploadError)

    def get_parent_folder_id(self, path):
        try:
            return DriveArchive.get(DriveArchive.path == parent_dir(path)).drive_id
        except DriveArchive.DoesNotExist:
            return self.get_drive_root_folder_id()

    def get_stored_file_id(self, path):
        try:
            return DriveArchive.get(DriveArchive.path == path).drive_id
        except DriveArchive.DoesNotExist:
            return None

    def dir_to_drive(self, path):
        entry = os.path.abspath(path)
        parent_id = self.get_parent_folder_id(entry)

        new_id = self.get_stored_file_id(entry)
        if new_id is None:
            new_id = self.my_google.create_folder(name_from_path(entry, raw=True), parent_id=parent_id)
            db_create(DriveArchive, path=entry, drive_id=new_id, date_modified_on_disk=date_modified(entry))
        return new_id

    def file_to_drive(self, path, folder_id=None):
        entry = os.path.abspath(path)
        if folder_id is None:
            folder_id = self.get_parent_folder_id(entry)
        file_id = self.get_stored_file_id(entry)

        resp = self.my_google.upload_file(path, folder_id=folder_id, file_id=file_id)
        try:
            db_create(DriveArchive, path=entry, drive_id=resp['id'], date_modified_on_disk=date_modified(entry))
        except IntegrityError:
            model = DriveArchive.get(DriveArchive.path == entry)
            db_update(model, path=entry, drive_id=resp['id'], date_modified_on_disk=date_modified(entry))

        return resp['id']

    def is_for_sync(self, path):
        entry = os.path.abspath(path)
        try:
            modified_date = DriveArchive.get(DriveArchive.path == entry).date_modified_on_disk
            return date_modified(entry) > modified_date
        except DriveArchive.DoesNotExist:
            return True

    def get_drive_root_folder_id(self):
        try:
            return self.config['GoogleDrive']['folder_id']
        except KeyError:
            if not self.my_google.get_file_data_by_name("Backuper"):
                folder_id = self.my_google.create_folder("Backuper")
            else:
                folder_id = self.my_google.get_file_data_by_name("Backuper")[0]['id']
            self.config['GoogleDrive']['folder_id'] = folder_id
        return folder_id

    def get_logs_folder_id(self):
        try:
            return self.config['GoogleDrive']['logs_folder_id']
        except KeyError:
            if self.my_google.get_file_data_by_name("Structure logs"):
                folder_id = self.my_google.get_file_data_by_name("Structure logs")[0]['id']
            else:
                folder_id = self.my_google.create_folder("Structure logs", parent_id=self.get_drive_root_folder_id())
            self.config['GoogleDrive']['logs_folder_id'] = folder_id
        return folder_id

    def read_paths_to_backup(self):
        all_paths = {}
        for section in self.config['Paths']:
            paths = [os.path.abspath(path) for path in self.config.get_section_values(self.config['Paths'][section])]
            all_paths[section] = paths
        return all_paths

    def get_paths_to_sync(self, path):
        for root, dirs, files in scandir.walk(path):
            if os.path.abspath(root) in self.blacklisted:
                continue
            if self.is_for_sync(root):
                yield os.path.abspath(root)
            for f in files:
                f_path = os.path.abspath(os.path.join(root, f))
                if f_path not in self.blacklisted and self.is_for_sync(f_path):
                    yield f_path

    def del_removed_from_local(self, log=False):
        deleted = 0
        for archive in DriveArchive.select():
            if not os.path.exists(archive.path) and self.my_google.exists(archive.drive_id):
                self.my_google.delete(archive.drive_id)

                if log:
                    dynamic_print("Removing {} from Google Drive".format(archive.path))

                deleted += DriveArchive.delete_instance(archive)
        return deleted

    def del_removed_from_drive(self, log=False):
        current = {drive_id['id'] for drive_id in self.my_google.list_all(fields="items/id")}
        archived = {archive.drive_id for archive in DriveArchive.select(DriveArchive.drive_id)}
        removed_from_drive = list(archived - current)

        for archive in DriveArchive.select().where(DriveArchive.drive_id << removed_from_drive):
            self.config['Paths']['blacklisted'] += archive.path + ';'

            dynamic_print("Added {} to blacklist".format(archive.path))

        return DriveArchive.delete().where(DriveArchive.drive_id << removed_from_drive).execute()

    # def delete_from_drive(self, path):
    #     path = os.path.abspath(path)
    #     if path in self.drive_archived:
    #         self.my_google.delete(self.drive_archived[path].id)
    #         if os.path.isdir(path):
    #             for root, dirs, files in scandir.walk(path):
    #                 self.drive_archived.pop(root)
    #                 for _file in files:
    #                     self.drive_archived.pop(os.path.join(root, _file))
    #         else:
    #             self.drive_archived.pop(path)

    def write_log_structure(self, save_to=".", path=".", dirs_only=False):
        file_name = r"{}\{}".format(save_to, name_from_path(path, ".txt"))
        with open(file_name, "w", encoding="utf8") as f:
            log_structure(path, dirs_only=dirs_only, output=f)

    @contextmanager
    def temp_dir(self, clean=True):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
            path_to_zip = zip_dir(temp_dir)
            self.my_google.upload(path_to_zip, folder_id=self.get_logs_folder_id())
            if clean:
                remove_file(path_to_zip)


class Dropbox:
    def __init__(self, overwrite=False):
        config = Config()
        ACCESSTOKEN = config["Dropbox"]["accesstoken"]
        try:
            self.client = dropbox.client.DropboxClient(ACCESSTOKEN)
        except ValueError as e:
            logging.critical(e, "\nFill in settings.ini")
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
                    logging.warning("connection error, ", e, " retrying")
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
    NUM_RETRIES = 5

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

    def handle_progressless_attempt(self, error, progressless_attempt, skip=True):
        if progressless_attempt > self.NUM_RETRIES:
            logging.critical('Failed to make progress.')
            if not skip:
                raise error
            else:
                return True

        sleeptime = 0.5 * (2**progressless_attempt)
        dynamic_print('Waiting for {}s before retry {}'.format(sleeptime, progressless_attempt))
        time.sleep(sleeptime)

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
        progressless_attempt = 0
        response = None
        while response is None:
            error = None
            try:
                progress, response = request.next_chunk(num_retries=500)
                if progress:
                    dynamic_print(self.progress_bar(progress, time_started), True)
            except HttpError as err:
                error = err
                if err.resp.status != 403:
                    logging.critical("HttpError response status: {}".format(err.resp.status))
                    raise
            except (httplib2.HttpLib2Error, IOError) as err:
                error = err

            if error:
                progressless_attempt += 1
                self.handle_progressless_attempt(error, progressless_attempt)
            else:
                progressless_attempt = 0

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
        date = self.get_metadata(file_id)['modifiedDate'].rsplit('.', 1)[0]
        if date:
            return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')

    def get_metadata(self, file_id, fields=None):
        try:
            return self.drive_service.files().get(fileId=file_id, fields=fields).execute()
        except HttpError as err:
            logging.warning(err)
            return None

    def get_file_data_by_name(self, name):
        return self.drive_service.files().list(q="title='{}'".format(name), fields='items').execute()['items']

    def list_all(self, fields=None):
        result = []
        page_token = None
        if fields:
            fields = 'nextPageToken,' + fields

        while True:
            param = {'fields': fields}
            if page_token:
                param['pageToken'] = page_token
            files = self.drive_service.files().list(**param).execute()

            result.extend(files['items'])
            page_token = files.get('nextPageToken')
            if not page_token:
                break

        return result

    def delete(self, file_id):
        progressless_attempt = 0
        while True:
            error = None
            try:
                return self.drive_service.files().delete(fileId=file_id).execute()
            except HttpError as err:
                error = err
                # if err.resp.status < 500:
                #     raise
            if error:
                progressless_attempt += 1
                if self.handle_progressless_attempt(error, progressless_attempt):
                    break
            else:
                progressless_attempt = 0

    def exists(self, file_id):
        if self.get_metadata(file_id) and not self.get_metadata(file_id, 'labels/trashed')['labels']['trashed']:
            return True
        return False


# TODO: uploading show file being uploaded on same line (\r) and progress for whole process not just for individual files
# TODO: multithreaded sync
# TODO: concurrent write_structure_to_file, seperate process for each for loop in main.py
# TODO: get all files to sync and show progress based on all files left
# TODO: create logger
# TODO: wrap every google operation with httperror protection
