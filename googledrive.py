import httplib2
from apiclient.discovery import build
from apiclient.http import MediaFileUpload, HttpRequest, MediaIoBaseDownload
from apiclient.errors import ResumableUploadError, HttpError
from oauth2client.client import OAuth2WebServerFlow, flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow
from oauth2client import tools

import os
import json
import time
import logging
import datetime
import mimetypes
from contextlib import ContextDecorator

from .sharedtools import *


NUM_RETRIES = 6
RETRYABLE_HTTP_ERROR_CODES = (403, 500)


class handle_http_error(ContextDecorator):
    def __init__(self, silent=False, suppress=False):
        self._suppress = suppress
        self._silent = silent

    def __enter__(self):
        pass

    def __exit__(self, exctype, excinst, exctb):
        if exctype is HttpError:
            logging.error(str(excinst))

            if self._suppress:
                return True

            if excinst.resp.status == 404:  # error 404 -> ignore (file is missing etc ...)
                return True

            if excinst.resp.status in RETRYABLE_HTTP_ERROR_CODES:
                for retry in range(NUM_RETRIES):
                    if handle_progressless_attempt(error, retry, retries=NUM_RETRIES):
                        if self._silent:
                            return True


_batch_error_counter = 0
def _batch_error_suppressor(request_id, response, exception):
    global _batch_error_counter
    if exception:
        logging.error(str(exception))
        print('counter: {}, error: {}'.format(_batch_error_counter, exception.resp.status))
        if exception.resp.status == 403:  # user limit reached -> exponential backoff
            _batch_error_counter += 1
            return handle_progressless_attempt(exception, _batch_error_counter, retries=NUM_RETRIES)
        else:
            _batch_error_counter = 0
    else:
        _batch_error_counter = 0


class GoogleDrive:
    UPLOAD_CHUNK_SIZE = 4 * 1024 ** 2
    DOWNLOAD_CHUNK_SIZE = 4 * 1024 ** 2
    BATCH_LIMIT = 5
    QPS_LIMIT = 10  # 10 queries per second
    CREDENTIALS_FILE = 'credentials.json'
    CLIENT_SECRET_FILE = 'client_secret.json'

    def __init__(self):
        credential_storage = Storage(self.CREDENTIALS_FILE)
        self.credentials = credential_storage.get()

        if self.credentials is None or self.credentials.invalid:
            flow = flow_from_clientsecrets(self.CLIENT_SECRET_FILE,
                                           scope="https://www.googleapis.com/auth/drive",
                                           redirect_uri="urn:ietf:wg:oauth:2.0:oob")
            flags = tools.argparser.parse_args(args=[])
            self.credentials = run_flow(flow, credential_storage, flags)

        self.drive_service = build('drive', 'v3', requestBuilder=self._build_request)

    def _build_request(self, _http, *args, **kwargs):
        # Create a new Http() object for every request
        http = self.credentials.authorize(httplib2.Http())
        return HttpRequest(http, *args, **kwargs)

    def print_progress_bar(self, status, time_started, type_bit):
        """
        Args:
            status: MediaDownloadProgress or MediaUploadProgress
            time_started: time.time()
            type_bit: if type_bit == 1 then output 'downloaded', if type_bit == 0 then output 'uploaded'

        Returns:
            None (prints to stdout using '\r')
        """

        time_left = ((time.time() - time_started) / status.progress()) - (time.time() - time_started)
        dynamic_print("{progress:.2f}% {status_type} [elapsed: {elapsed}, left: {left}]".format(
                progress=status.progress() * 100,
                status_type="downloaded" if type_bit else "uploaded",
                elapsed=format_seconds(time.time() - time_started),
                left=format_seconds(time_left)), log=False, fit=True)

    def download(self, file_id, save_path, filename=None):
        """
        Download a file.

        Args:
            file_id: file id
            save_path: str to a directory
            filename: join filename to save_path if given, otherwise fetch file name from Google Drive
        Returns:
            if successful: str, download path
            else: file_id
        """

        if filename is None:
            filename = self.get_metadata(file_id, fields="name")
            if filename:
                filename = filename['name']
            else:
                return file_id

        os.makedirs(save_path, exist_ok=True)
        download_path = os.path.abspath(os.path.join(save_path, filename))

        logging.info("Downloading {} to {}".format(file_id, download_path))

        request = self.drive_service.files().get_media(fileId=file_id)
        with open(download_path, 'wb') as file:
            downloader = MediaIoBaseDownload(file, request, chunksize=self.DOWNLOAD_CHUNK_SIZE)

            time_started = time.time()
            done = False
            while not done:
                status, done = downloader.next_chunk(num_retries=NUM_RETRIES)
                self.print_progress_bar(status, time_started, 1)

        return download_path

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
            'name': real_case_filename(file_path),
            'parents': [folder_id]
        }

        if getsize(file_path):
            media_body = MediaFileUpload(file_path, mimetype=mime, chunksize=self.UPLOAD_CHUNK_SIZE, resumable=True)
        else:
            return self.drive_service.files().create(body=body).execute()

        request = self._determine_update_or_insert(body, media_body, file_id=file_id)

        time_started = time.time()
        response = None
        while response is None:
            with handle_http_error(silent=True, suppress=False):
                progress, response = request.next_chunk(num_retries=5)
                if progress:
                    self.print_progress_bar(progress, time_started, 0)

        return response

    def _determine_update_or_insert(self, body, media_body, file_id=None):
        if file_id:
            body.pop('parents')
            return self.drive_service.files().update(fileId=file_id, body=body, media_body=media_body)
        return self.drive_service.files().create(body=body, media_body=media_body)

    @uploading_to('Google Drive', dynamic=True)
    def upload_directory(self, dir_path, root_id='root'):
        archived_dirs = {}
        for root, dirs, files in walk(dir_path):
            parent_id = archived_dirs.get(parent_dir(root), root_id)

            try:
                dir_id = archived_dirs[os.path.abspath(root)]
            except KeyError:
                dir_id = self.create_folder(real_case_filename(root), parent_id=parent_id)
                archived_dirs[os.path.abspath(root)] = dir_id

            for _file in files:
                self.upload_file(os.path.join(root, _file), folder_id=dir_id)

    @handle_http_error(suppress=True)
    def create_folder(self, name, parent_id='root'):
        body = {
            'name': name,
            'parents': [parent_id],
            'mimeType': 'application/vnd.google-apps.folder'
        }

        return self.drive_service.files().create(body=body).execute()['id']

    def get_modified_time(self, file_id):
        date = self.get_metadata(file_id)['modifiedTime'].rsplit('.', 1)[0]
        if date:
            return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')

    @handle_http_error(suppress=True)
    def get_metadata(self, file_id, fields=None):
        return self.drive_service.files().get(fileId=file_id, fields=fields).execute()

    def update_metadata(self, file_id, fields=None, **kwargs):
        if kwargs:
            return self.drive_service.files().update(fileId=file_id, body=kwargs, fields=fields).execute()

    def get_file_data_by_name(self, name):
        return self.drive_service.files().list(q="name='{}'".format(name), fields='files').execute()['files']

    def list_all(self, **kwargs):
        result = []

        files_request = self.drive_service.files().list(**kwargs)
        while files_request is not None:
            files_resp = files_request.execute()
            result.extend(files_resp['files'])
            files_request = self.drive_service.files().list_next(files_request, files_resp)

        return result

    def batch_delete(self, ids):
        _batch = self.drive_service.new_batch_http_request(callback=_batch_error_suppressor)
        requests_in_batch = 0
        for _id in ids:
            if requests_in_batch >= self.BATCH_LIMIT:
                _batch.execute()
                requests_in_batch = 0

            _batch.add(self.drive_service.files().delete(fileId=_id))
            requests_in_batch += 1

        if requests_in_batch > 0:
            _batch.execute()

    @handle_http_error(suppress=False)
    def delete(self, file_id):
        self.drive_service.files().delete(fileId=file_id).execute()

    def exists(self, file_id):
        metadata = self.get_metadata(file_id, fields='trashed')
        if metadata:
            if not metadata['trashed']:
                return True
        return False

    # @handle_http_error()
    def get_start_page_token(self):
        return int(self.drive_service.changes().getStartPageToken().execute()["startPageToken"])

    # @handle_http_error(suppress=True)
    def get_changes(self, start_page_token=None, fields=None):
        """
        yield response of all changes since start_page_token
        """

        page_token = start_page_token

        if fields:
            if "nextPageToken" not in fields:
                fields = "nextPageToken," + fields
            if "newStartPageToken" not in fields:
                fields = "newStartPageToken," + fields

        param = {'fields': fields, 'restrictToMyDrive': True, 'pageSize': 100, 'pageToken': start_page_token}

        while page_token is not None:
            if page_token:
                param['pageToken'] = page_token

            changes_request = self.drive_service.changes().list(**param).execute()

            for change in changes_request['changes']:
                yield change

            if "newStartPageToken" in changes_request:
                return

            page_token = changes_request.get('nextPageToken')
