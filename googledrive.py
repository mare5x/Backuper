import httplib2
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from apiclient.errors import ResumableUploadError, HttpError
from oauth2client.client import OAuth2WebServerFlow
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


NUM_RETRIES = 5


class handle_http_error(ContextDecorator):
    def __init__(self, silent=False, suppress=False):
        self._suppress = suppress
        self._silent = silent

    def __enter__(self):
        pass

    def __exit__(self, exctype, excinst, exctb):
        if exctype is HttpError:
            for retry in range(NUM_RETRIES):
                if excinst.resp.status != 403:
                    try:
                        error = json.loads(excinst.content.decode('utf-8'))
                    except ValueError:
                        # could not load json
                        error = {}

                    logging.error(error.get('code', excinst.resp.status))
                    logging.error(error.get('message'))

                if self._suppress:
                    return True

                if handle_progressless_attempt(error, retry, retries=NUM_RETRIES):
                    if self._silent:
                        return True
                    return False

            return False


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
            with handle_http_error(silent=True, suppress=False):
                progress, response = request.next_chunk(num_retries=5)
                if progress:
                    dynamic_print(self.progress_bar(progress, time_started), True)

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

    @handle_http_error(suppress=True)
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
        with handle_http_error(suppress=True):
            return self.drive_service.files().get(fileId=file_id, fields=fields).execute()

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

    @handle_http_error(suppress=True)
    def delete(self, file_id):
        return self.drive_service.files().delete(fileId=file_id).execute()

    def exists(self, file_id):
        if self.get_metadata(file_id) and not self.get_metadata(file_id, 'labels/trashed')['labels']['trashed']:
            return True
        return False

    def get_changes(self, start_change_id=None, fields=None):
        result = dict()
        page_token = None
        param = {'fields': fields, 'includeSubscribed': False, 'maxResults': 1000, 'startChangeId': start_change_id}
        while True:
            if page_token:
                param['pageToken'] = page_token

            changes = {}
            with handle_http_error(suppress=True):
                changes = self.drive_service.changes().list(**param).execute()

            result.update(changes)

            page_token = changes.get('nextPageToken')
            if not page_token:
                break
        return result

    @handle_http_error(suppress=True)
    def get_change(self, change_id, fields=None):
        return self.drive_service.changes().get(changeId=change_id, fields=fields).execute()
