import httplib2
from apiclient.discovery import build
from apiclient.http import MediaFileUpload, HttpRequest
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


class handle_http_error(ContextDecorator):
    def __init__(self, silent=False, suppress=False):
        self._suppress = suppress
        self._silent = silent

    def __enter__(self):
        pass

    def __exit__(self, exctype, excinst, exctb):
        if exctype is HttpError:
            logging.error(str(excinst))

            if self._suppress or excinst.resp.status == 404:  # error 404 -> ignore (file is missing etc ...)
                return True

            for retry in range(NUM_RETRIES):
                # if excinst.resp.status != 403:
                #     try:
                #         error = json.loads(excinst.content.decode('utf-8'))
                #     except ValueError:
                #         # could not load json
                #         error = {}

                #     logging.error(error.get('code', excinst.resp.status))
                #     logging.error(error.get('message'))

                if handle_progressless_attempt(error, retry, retries=NUM_RETRIES):
                    if self._silent:
                        return True
                    return False

            return False


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
    CHUNK_SIZE = 4 * 1024 ** 2
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

    def progress_bar(self, status, time_started):
        time_left = ((time.time() - time_started) / status.progress()) - (time.time() - time_started)
        return "{:.2f}% uploaded [elapsed: {}, left: {}]".format(status.progress() * 100,
                                                                 format_seconds(time.time() - time_started),
                                                                 format_seconds(time_left))

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
            'name': name_from_path(file_path, raw=True),
            'parents': [folder_id]
        }

        if getsize(file_path):
            media_body = MediaFileUpload(file_path, mimetype=mime, chunksize=self.CHUNK_SIZE, resumable=True)
        else:
            return self.drive_service.files().create(body=body).execute()

        request = self._determine_update_or_insert(body, media_body, file_id=file_id)

        time_started = time.time()
        response = None
        while response is None:
            with handle_http_error(silent=True, suppress=False):
                progress, response = request.next_chunk(num_retries=5)
                if progress:
                    dynamic_print(self.progress_bar(progress, time_started), True)

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
                dir_id = self.create_folder(name_from_path(root, raw=True), parent_id=parent_id)
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
                time.sleep(1)
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
