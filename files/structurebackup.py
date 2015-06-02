#! python3

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


class Config(configparser.ConfigParser):
    def __init__(self):
        super().__init__()

        if not self.read('settings.ini'):
            self.make_layout()
            self.write_to_config()

    def make_layout(self):
        self['Dropbox'] = {
            'appkey': '',
            'appsecret': '',
            'accesstoken': ''
        }

        self['GoogleDrive'] = {
            'client_id': '',
            'client_secret': '',
            'oauth_scope': '',
            'redirect_uri': ''
        }

    def write_to_config(self):
        with open('settings.ini', 'w') as configfile:
            self.write(configfile)

    def get_shelf(self, key):
        with shelve.open('settings') as db:
            return db[key]

    def set_shelf(self, key, item):
        with shelve.open('settings') as db:
            db[key] = item


class Backup():
    def __init__(self, save_to_path=".", clean=False, my_dropbox=None, my_google=None):
        self.temp_dir_path = save_to_path
        self.clean = clean
        self.my_dropbox = my_dropbox
        self.my_google = my_google

    def __enter__(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = self.temp_dir.name
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        path_to_zip = zip_dir(self.temp_dir_path)
        if self.my_dropbox:
            self.my_dropbox.upload_file(path_to_zip)
        if self.my_google:
            self.my_google.upload_file(path_to_zip)
        if self.clean:
            remove_file(path_to_zip)
        print("\nDONE")


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

    def upload_file(self, file_path):
        with open(file_path, "rb") as f:
            print("uploading {} ({}) to dropbox".format(file_path, get_file_size(file_path)))
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
                self.client.file_move(old_file, "{}.zip".format(get_date(for_file=True)))
            else:
                uploader.finish("{}.zip".format(get_date(for_file=True)))


class DropboxUploader(dropbox.client.ChunkedUploader):
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
        config = Config()
        CLIENT_ID = config['GoogleDrive']['client_id']
        CLIENT_SECRET = config['GoogleDrive']['client_secret']
        OAUTH_SCOPE = config['GoogleDrive']['oauth_scope']
        REDIRECT_URI = config['GoogleDrive']['redirect_uri']
        try:
            credentials = config.get_shelf('credentials')
        except KeyError:
            credentials = config.set_shelf('credentials', None)

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
                config.set_shelf('credentials', credentials)
                continue
            break

        self.drive_service = build('drive', 'v2', http=http)

    def upload_file(self, file_path):
        media_body = MediaFileUpload(file_path, chunksize=4 * 1024 ** 2, resumable=True)
        body = {
            'title': name_from_path(file_path, raw=True)
        }
        self.drive_service.files().insert(body=body, media_body=media_body).execute()
