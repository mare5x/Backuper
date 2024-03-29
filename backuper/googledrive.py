import httplib2
from apiclient.discovery import build
from apiclient.http import MediaFileUpload, HttpRequest, MediaIoBaseDownload
from apiclient.errors import ResumableUploadError, HttpError
from oauth2client.client import OAuth2WebServerFlow, flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow
from oauth2client import tools

import os
import re
import json
import time
import math
import logging
import datetime
import mimetypes
import concurrent.futures
from functools import wraps

from pytools import filetools as ft
from pytools import progressbar, cache


NUM_RETRIES = 6
RETRYABLE_HTTP_ERROR_CODES = (403, 500)


def handle_http_error(silent=False, ignore=False):
    """Decorator that handles HttpErrors by retrying the decorated function.
    
    Keyword arguments:
        silent: don't raise error if all retries fail (default False)
        ignore: upon HttpError, ignore it without retrying (default False)
    Returns:
        on success: return what the decorated functions returns
        on HttpError: return None if ignore=True or silent=True
    """
    def decorated(func):
        @wraps(func)
        def inner_decorated(*args, **kwargs):
            error = None
            for attempt in range(1, NUM_RETRIES + 1):
                try:
                    return func(*args, **kwargs)
                except HttpError as e:
                    error = e
                    if ignore or e.resp.status == 404:
                        logging.info("Ignoring error {}".format(e))
                        return
                    
                    if e.resp.status in RETRYABLE_HTTP_ERROR_CODES:
                        sleeptime = 2 ** attempt
                        time.sleep(sleeptime)
                    
                    logging.info("Retrying {func}({args}) due to error {error}".format(func=func.__name__, 
                                                                                   args=(args, kwargs), 
                                                                                   error=e))
            if silent:
                logging.error("Silenced error {}".format(error))
                return
            else:
                raise error
                
        return inner_decorated
    return decorated


def convert_google_time_to_datetime(google_time):
    return datetime.datetime.strptime(google_time.rsplit('.', 1)[0], '%Y-%m-%dT%H:%M:%S')

def convert_datetime_to_google_time(dtime):
    return dtime.isoformat(sep='T', timespec="microseconds") + 'Z'


class GoogleDrive:
    # Note: https://developers.google.com/apis-explorer/#p/drive/v3/ is very handy!

    UPLOAD_CHUNK_SIZE = 4 * 1024 ** 2
    DOWNLOAD_CHUNK_SIZE = 4 * 1024 ** 2
    BATCH_LIMIT = 32
    CREDENTIALS_FILE = 'credentials.json'
    CLIENT_SECRET_FILE = 'client_secret.json'

    FOLDER_MIMETYPE = 'application/vnd.google-apps.folder'

    def __init__(self):
        credential_storage = Storage(self.CREDENTIALS_FILE)
        self.credentials = credential_storage.get()

        if self.credentials is None or self.credentials.invalid:
            flow = flow_from_clientsecrets(self.CLIENT_SECRET_FILE,
                                           scope="https://www.googleapis.com/auth/drive",
                                           redirect_uri="urn:ietf:wg:oauth:2.0:oob")
            flags = tools.argparser.parse_args(args=[])
            self.credentials = run_flow(flow, credential_storage, flags)

        self.drive_service = build('drive', 'v3', credentials=self.credentials, requestBuilder=self._build_request)

        # file_id -> metadata response cache.
        self.metadata_cache = cache.LRUcache(32768)  # 2^15

    def _build_request(self, _http, *args, **kwargs):
        # Create a new Http() object for every request
        http = self.credentials.authorize(httplib2.Http())
        return HttpRequest(http, *args, **kwargs)

    def exit(self): 
        print("Metadata cache hits/misses:", self.metadata_cache.hits, self.metadata_cache.misses)

    def walk_folder(self, folder_id, dirname=None, dirpath="", fields="files(id, name)", q=None):
        """Recursively yield all content in folder_id (similar to os.walk). 
        
        Positional arguments:
            folder_id: str, Google Drive id of folder
        Keyword arguments:
            dirname: str, name of folder_id
            dirpath: str, path prefix (default "")
            fields: str, fields to use when requesting the Google Drive API (default 'files(id, name)')
            q: str, query to be used when requesting the Google Drive API. ONLY for filenames. (default None)
        Yields:
            a 3-tuple (dirpath, dirnames, filenames),
            where dirnames and filenames are lists of metadata responses.
            And dirpath is a 2-tuple (dirpath, folder_id).
        """
        if dirname is None:
            dirname = self.get_id_name(folder_id)
            if not dirname:
                return

        fields = self._merge_fields(fields, "files(id,name)")
        dirpath = os.path.join(dirpath, dirname)

        # It's faster to make fewer API requests ...
        dirs = []
        files = []
        for ftype, resp in self.get_all_in_folder(folder_id, fields=fields, q=q):
            if ftype == "#file": files.append(resp)
            else: dirs.append(resp)

        yield (dirpath, folder_id), dirs, files

        for dir_response in dirs:
            yield from self.walk_folder(dir_response["id"], dirname=dir_response["name"], dirpath=dirpath, fields=fields, q=q)

    def create_local_folder(self, path):
        os.makedirs(path, exist_ok=True)
        return path

    def download_folder(self, folder_id, save_path, folder_name=None):
        """Recursively download a folder and all its content.

        Args:
            folder_id: folder id
            save_path: str to a directory
            folder_name: join folder_name to save_path if given, otherwise fetch folder name from Google Drive
        """
        for dirpath, dirnames, filenames in self.walk_folder(folder_id, dirname=folder_name):
            dir_path, dir_id = dirpath
            dl_root = os.path.join(save_path, dir_path)
            self.create_local_folder(dl_root)
            for filename in filenames:
                self.download_file(filename["id"], dl_root, filename=filename["name"])

    def download_file(self, file_id, save_path, filename=None):
        """Download a file.

        Args:
            file_id: file id
            save_path: str to a directory
            filename: join filename to save_path if given, otherwise fetch file name from Google Drive
        Returns:
            if successful: str, download path
            else: None
        """
        if filename is None:
            filename = self.get_id_name(file_id)
            if not filename:
                return

        self.create_local_folder(save_path)
        download_path = os.path.abspath(os.path.join(save_path, filename))

        logging.info("GD DL: {} -> {}".format(file_id, download_path))

        request = self.drive_service.files().get_media(fileId=file_id)
        with open(download_path, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request, chunksize=self.DOWNLOAD_CHUNK_SIZE)

            pbar = progressbar.blockbar(desc="DL " + filename, bar_width=12)
            done = False
            while not done:
                try:
                    status, done = downloader.next_chunk(num_retries=NUM_RETRIES)
                except HttpError as e:
                    # "Request range not satisfiable" error
                    # This is a bug in MediaIoBaseDownload. It happens when
                    # trying to download a file with size 0 bytes. The error
                    # is safe to ignore.
                    if e.resp.status == 416:
                        logging.warning(e)
                        break
                    raise e

                pbar.set_progress(status.progress() if status else 1)
            pbar.close()

        return download_path

    def upload(self, path, folder_id='root', file_id=None, fields=None):
        if os.path.isdir(path):
            return self.upload_directory(path, root_id=folder_id)
        return self.upload_file(path, folder_id=folder_id, file_id=file_id, fields=fields)

    def upload_file(self, file_path, folder_id='root', file_id=None, fields=None):
        """If file_id is specified, the file will be updated/patched."""

        logging.info("GD UL: {}".format(file_path))

        mime, encoding = mimetypes.guess_type(file_path)
        if mime is None:
            mime = 'application/octet-stream'
        
        body = {
            'name': ft.real_case_filename(file_path),
            'parents': [folder_id]
        }

        # Empty files can't be uploaded with chunks because they aren't resumable.
        resumable = ft.getsize(file_path) > 0
        media_body = MediaFileUpload(file_path, mimetype=mime, chunksize=self.UPLOAD_CHUNK_SIZE, resumable=resumable)
        request = self._determine_update_or_insert(body, media_body=media_body, file_id=file_id, fields=fields)
        
        pbar = progressbar.blockbar(desc="UL " + body["name"], bar_width=12)
        response = None if resumable else request.execute()  # Empty files are not chunked.
        while response is None:
            status, response = request.next_chunk(num_retries=5)
            pbar.set_progress(status.progress() if status else 1)
        pbar.close()

        return response

    def _determine_update_or_insert(self, body, media_body=None, file_id=None, **kwargs):
        # googleapiclient.http.HttpRequest object returned.
        if file_id:
            body.pop('parents')  # update requests can't have the parents attribute.
            return self.drive_service.files().update(fileId=file_id, body=body, media_body=media_body, **kwargs)
        return self.drive_service.files().create(body=body, media_body=media_body, **kwargs)

    def upload_directory(self, dir_path, root_id='root'):
        archived_dirs = {}
        for root, dirs, files in os.walk(dir_path):
            parent_id = archived_dirs.get(ft.parent_dir(root), root_id)

            try:
                dir_id = archived_dirs[os.path.abspath(root)]
            except KeyError:
                dir_id = self.create_folder(ft.real_case_filename(root), parent_id=parent_id)
                archived_dirs[os.path.abspath(root)] = dir_id

            for _file in files:
                self.upload_file(os.path.join(root, _file), folder_id=dir_id)
        return archived_dirs[os.path.abspath(dir_path)]

    @handle_http_error(ignore=False)
    def create_folder(self, name, parent_id='root'):
        logging.info("GD UL DIR: {}".format(name))

        body = {
            'name': name,
            'parents': [parent_id],
            'mimeType': GoogleDrive.FOLDER_MIMETYPE
        }

        return self.drive_service.files().create(body=body).execute()['id']

    def get_modified_time(self, file_id):
        date = self.get_metadata(file_id)['modifiedTime'].rsplit('.', 1)[0]
        if date:
            return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
            
    def get_remote_path(self, file_id, stop_id=None):
        if file_id == stop_id: return os.path.sep
        
        # NOTE: metadata is cached!
        resp = self.get_metadata(file_id, fields="name,parents")
        if not resp:  # There was an error. Most likely file_id doesn't exist.
            raise RuntimeError("Error retrieving metadata! Does {} exist?".format(file_id))

        parent_id = resp["parents"][0] if "parents" in resp else stop_id
        path = os.path.join(self.get_remote_path(parent_id, stop_id), resp["name"])

        return path

    @handle_http_error(ignore=False)
    def get_metadata(self, file_id, fields=None):
        resp = self.metadata_cache.get(file_id)
        
        if resp is not None:
            if fields is None:
                return resp
            
            # Are there any missing fields?
            obj = self._parse_fields_string(fields)
            missing = False
            for key in obj:
                if key not in resp:
                    missing = True
                    break
            if not missing:
                return resp

        new_resp = self.drive_service.files().get(fileId=file_id, fields=fields).execute()
        
        if resp is not None:
            resp.update(new_resp)
        else:
            resp = new_resp
        self.metadata_cache[file_id] = resp
        
        return resp

    @handle_http_error(ignore=False)
    def update_metadata(self, file_id, fields=None, **kwargs):
        if kwargs:
            return self.drive_service.files().update(fileId=file_id, body=kwargs, fields=fields).execute()

    @handle_http_error(ignore=False)
    def move_file(self, src_id, dest_id):
        """Move src_id to be a child of dest_id."""
        data = self.get_metadata(src_id, fields="parents")
        parents = ",".join(data.get("parents"))
        self.drive_service.files().update(fileId=src_id, fields="id, parents", addParents=dest_id, removeParents=parents).execute()

    def rename_file(self, file_id, name):
        self.update_metadata(file_id, name=name)

    def get_id_name(self, file_id):
        filename = self.get_metadata(file_id, fields="name")
        if filename:
            return filename['name']

    def get_file_by_name(self, name, fields="files(id, name, parents)"):
        return self.drive_service.files().list(q="name='{}'".format(name), fields=fields).execute()["files"]

    def get_all_in_folder(self, folder_id, fields="files(trashed,id,name)", q=None):
        """Yields all (non-trashed) files in a folder (direct children) with fields metadata.
        Yields: (#file or #folder, resp) pairs."""
        
        fields = self._merge_fields(fields, 'files(trashed,id,mimeType),nextPageToken')
        search_query = "'{folder_id}' in parents".format(folder_id=folder_id)
        if q:
            search_query = "{search_query} and ({user_q})".format(search_query=search_query, user_q=q)

        request = self.drive_service.files().list(q=search_query, fields=fields, pageSize=500)
        while request is not None:
            response = request.execute()

            for file in response['files']:
                if not file['trashed']:
                    file.pop('trashed')
                    _type = "#folder" if file["mimeType"] == self.FOLDER_MIMETYPE else "#file"
                    yield (_type, file)

            request = self.drive_service.files().list_next(request, response)

    def get_files_in_folder(self, folder_id, fields="files(trashed, id, name)", q=None):
        """Yields all (non-trashed) files in a folder (direct children) with fields metadata. 
        Doesn't include folders."""
        
        fields = self._merge_fields(fields, 'files(trashed,id),nextPageToken')
        search_query = "mimeType!='{}' and '{folder_id}' in parents".format(GoogleDrive.FOLDER_MIMETYPE, folder_id=folder_id)
        if q:
            search_query = "{search_query} and ({user_q})".format(search_query=search_query, user_q=q)

        request = self.drive_service.files().list(q=search_query, fields=fields, pageSize=500)
        while request is not None:
            response = request.execute()

            for file in response['files']:
                if not file['trashed']:
                    file.pop('trashed')
                    yield file

            request = self.drive_service.files().list_next(request, response)

    def get_folders_in_folder(self, folder_id, fields="files(trashed, id, name)", q=None):
        """Yields all (non-trashed) folders in a folder (direct children) with fields metadata."""
        
        fields = self._merge_fields(fields, 'files(trashed,id),nextPageToken')
        search_query = "mimeType='{}' and '{folder_id}' in parents".format(GoogleDrive.FOLDER_MIMETYPE, folder_id=folder_id)
        if q:
            search_query = "{search_query} and ({user_q})".format(search_query=search_query, user_q=q)

        request = self.drive_service.files().list(q=search_query, fields=fields, pageSize=500)
        while request is not None:
            response = request.execute()

            for folder in response['files']:
                if not folder['trashed']:
                    folder.pop('trashed')
                    yield folder

            request = self.drive_service.files().list_next(request, response)

    def get_parent_id(self, file_id):
        resp = self.get_metadata(file_id, fields="name,parents")  # same fields as remote path for caching
        if resp and "parents" in resp: 
            return resp["parents"][0]
        return None
    
    def get_parents(self, file_id):
        """NOTE: files are assumed to have at most ONE parent!"""
        parent_id = file_id
        while parent_id:
            yield parent_id
            parent_id = self.get_parent_id(parent_id)

    def is_parent(self, folder_id, file_id):
        """Whether folder_id is a not necessarily direct parent of file_id."""
        for parent in self.get_parents(file_id):
            if parent == folder_id:
                return True
        return False

    def _parse_fields_string(self, fields):
        """Parse a valid string of 'fields' into a dict object."""
        sep = ",/()"

        # 1. remove all whitespace.
        _fields = ""
        for c in fields:
            if not c.isspace():
                _fields += c
        fields = _fields
        
        i = 0
        n = len(fields)

        def parse(start):
            nonlocal i, n
            obj = dict()
            while i < n:
                cur = ''
                while i < n and fields[i] not in sep:
                    cur += fields[i]
                    i += 1

                obj[cur] = 0
                if i == n: 
                    break

                if fields[i] == '/':
                    i += 1
                    obj[cur] = parse(i - 1)
                elif fields[i] == '(':
                    i += 1
                    obj[cur] = parse(i - 1)
                
                if i == n:
                    break

                if fields[i] == ',':
                    if fields[start] == '/':
                        break
                elif fields[i] == ')':
                    if fields[start] == '(':
                        i += 1
                    break

                i += 1

            return obj

        return parse(0)

    def _parse_fields_dict(self, obj):
        """Convert an object returned by _parse_fields_string (or a response object) 
        into a valid 'fields' string."""
        fields = ""
        for key, value in obj.items():
            fields += key
            if isinstance(value, dict):
                fields += '/' if len(value) == 1 else '('
                fields += self._parse_fields_dict(value)
                if len(value) > 1: fields += ')'
            fields += ','
        return fields.rstrip(',')

    def _merge_fields(self, fields1, fields2):
        """Merge valid 'fields' strings into a single valid 'fields' string."""
        obj1 = self._parse_fields_string(fields1)
        obj2 = self._parse_fields_string(fields2)

        def merge_dicts(d1, d2):
            for k2, v2 in d2.items():
                v1 = d1.get(k2)
                if v1 is None:
                    d1[k2] = v2
                else:
                    # The key exists in both dicts.
                    t1, t2 = type(v1), type(v2)
                    if t1 is dict and t2 is dict:
                        d1[k2] = merge_dicts(v1, v2)
                    elif t1 is dict:
                        d1[k2] = v1
                    elif t2 is dict:
                        d1[k2] = v2
                    else:
                        d1[k2] = v2
            return d1
        
        obj = merge_dicts(obj1, obj2)
        return self._parse_fields_dict(obj)

    def list_all(self, **kwargs):
        result = []

        files_request = self.drive_service.files().list(**kwargs)
        while files_request is not None:
            files_resp = files_request.execute()
            result.extend(files_resp['files'])
            files_request = self.drive_service.files().list_next(files_request, files_resp)

        return result

    def batch_delete(self, file_ids, callback=None):
        """callback: callable, A callback to be called for each response, of the
        form callback(file_id, response, exception). The first parameter is the
        file id, and the second is the deserialized response object. The
        third is an googleapiclient.errors.HttpError exception object if an HTTP error
        occurred while processing the request, or None if no error occurred.
        """
        batch = self.drive_service.new_batch_http_request()
        requests_in_batch = 0
        for file_id in file_ids:
            if requests_in_batch >= self.BATCH_LIMIT:
                batch.execute()
                batch = self.drive_service.new_batch_http_request()
                requests_in_batch = 0

            request = self.drive_service.files().delete(fileId=file_id)
            # File ids are unique so we can use them as request ids.
            request_id = file_id if callback else None
            batch.add(request, callback=callback, request_id=request_id)
            requests_in_batch += 1

        if requests_in_batch > 0:
            batch.execute()

    # @handle_http_error(ignore=False)
    def delete(self, file_id):
        try:
            self.drive_service.files().delete(fileId=file_id).execute()
            logging.info("GD DELETE: %s", file_id)
        except HttpError as e:
            if e.resp.status == 404:  # File doesn't exist. Safe to ignore.
                logging.warning("GD DELETE: IGNORING " + repr(e))
            else:
                raise e

    def exists(self, file_id):
        metadata = self.get_metadata(file_id, fields='trashed')
        if metadata:
            if not metadata['trashed']:
                return True
        return False

    @handle_http_error(ignore=False)
    def get_start_page_token(self):
        return int(self.drive_service.changes().getStartPageToken().execute()["startPageToken"])

    # @handle_http_error(ignore=True)
    def get_changes(self, start_page_token=None, fields=None, include_removed=True):
        """Yield response of all changes since start_page_token.
        NOTE: if include_removed is True, trashed files will still be shown."""

        page_token = start_page_token

        if fields:
            if "nextPageToken" not in fields:
                fields = "nextPageToken," + fields
            if "newStartPageToken" not in fields:
                fields = "newStartPageToken," + fields

        param = {'fields': fields, 'restrictToMyDrive': True, 'pageSize': 500, 'pageToken': start_page_token, 'includeRemoved': include_removed}

        while page_token is not None:
            if page_token:
                param['pageToken'] = page_token

            changes_request = self.drive_service.changes().list(**param).execute()

            for change in changes_request['changes']:
                yield change

            if "newStartPageToken" in changes_request:
                return

            page_token = changes_request.get('nextPageToken')


class PPGoogleDrive(GoogleDrive):
    """Wrapper around GoogleDrive that pretty-prints GD file transfers."""

    SECTION_NAMES = ["OPERATION", "FILE ID", "REMOTE PATH", "LOCAL PATH"]
    SECTION_WIDTHS = [9, 33, 40, 80]

    UNKNOWN_FIELD = "---"

    LOG_LEVEL = 1

    def __init__(self, stream=None, filename=None, **kwargs):
        """Pretty print output to stream or to a file."""
        super().__init__()

        if stream is not None and filename is not None:
            raise ValueError("'stream' and 'filename' should not be specified together")

        # For printing we will abuse the logging module.
        # That way we don't have to handle thread safety manually.
        self.logger = logging.getLogger("PPGoogleDrive")
        self.logger.propagate = False
        self.logger.setLevel(self.LOG_LEVEL)
        if filename is not None:
            handler = logging.FileHandler(filename, 
                mode=kwargs.get("mode", "w"), encoding=kwargs.get("encoding", "utf8"))
        if stream is not None:
            handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter(fmt="%(message)s"))
        handler.terminator = ''
        self.logger.addHandler(handler)

        self.remote_update_bytes = 0
        self.remote_new_bytes = 0
        self.remote_new_count = 0
        self.remote_update_count = 0
        self.remote_delete_count = 0
        self.download_count = 0
        self.downloaded_bytes = 0
        self.time_started = time.time()

        self.write_header()

    def exit(self):
        self.write_footer()
        for h in self.logger.handlers:
            h.close()
        super().exit()

    def write(self, msg):
        self.logger.log(self.LOG_LEVEL, msg)

    def write_header(self):
        self.write("\n{}\n\n".format(time.strftime("%Y %b %d %H:%M:%S", time.gmtime())))
        self.write_line(*self.SECTION_NAMES, min_rows=3)
        self.write_line(*['-' * width for width in self.SECTION_WIDTHS])
    
    def write_footer(self):        
        self.write("\nTIME ELAPSED: " + ft.format_seconds(time.time() - self.time_started) + '\n')

        widths = [16, 6, 10]
        self.write_table_row(self, ['-' * w for w in widths], widths)
        self.write_table_row(self, 
            ["NEWER", str(self.remote_new_count), ft.convert_file_size(self.remote_new_bytes)], 
            widths)
        self.write_table_row(self, 
            ["UPDATED", str(self.remote_update_count), ft.convert_file_size(self.remote_update_bytes)], 
            widths)
        self.write_table_row(self,
            ["DELETED", str(self.remote_delete_count), self.UNKNOWN_FIELD],
            widths)
        self.write_table_row(self,
            ["DOWNLOADED", str(self.download_count), ft.convert_file_size(self.downloaded_bytes)],
            widths)
        self.write_table_row(self, ['-' * w for w in widths], widths)

    @staticmethod
    def write_table_row(stream, sections, section_widths, center_cols=True, center_rows=True, min_rows=0, sep='|'):
        """Each section has a max width. If it exceeds that limit, it is 
        printed in the next line. Text in each section can be centerd 
        horizontally or vertically. If min_rows is specified, each row will have 
        at least that many rows.
        """
        n = len(sections)
        n_rows = max((math.ceil(len(sections[i]) / section_widths[i]) for i in range(n)))
        n_rows = max(n_rows, min_rows)
        sections_start_idx = [0] * n  # If center_rows, at which row does each section start.
        if center_rows:
            for i in range(n):
                span = math.ceil(len(sections[i]) / section_widths[i])
                sections_start_idx[i] = (n_rows - span) // 2

        for row_idx in range(n_rows):
            line = ""
            for j in range(n):
                width = section_widths[j]
                section = sections[j]
                section_len = len(section)
                start_idx = width * (row_idx - sections_start_idx[j])
                if start_idx >= 0 and start_idx < section_len:
                    end_idx = min(section_len, start_idx + width)
                    written = end_idx - start_idx
                    if center_cols:
                        offset = ((width - written) // 2) if (written < width) else 0
                        line += ' ' * offset
                        width -= offset
                    line += section[start_idx:end_idx]
                    width -= written
                line += ' ' * width
                if j < n - 1:
                    line += sep
            stream.write(line + '\n')

    def write_line(self, operation, file_id, remote_path, local_path, **kwargs):
        sections = [operation, file_id, remote_path, local_path]
        self.write_table_row(self, sections, self.SECTION_WIDTHS, **kwargs)

    def upload_file(self, file_path, folder_id='root', file_id=None, fields=None):
        # Override.
        fields = self._merge_fields(fields or '', "id,name,size")
        resp = super().upload_file(file_path, folder_id=folder_id, file_id=file_id, fields=fields)

        operation = "UPDATE" if file_id else "NEW"
        file_id = resp["id"]
        remote_path = self.get_remote_path(file_id)
        local_path = file_path
        if operation == "NEW":
            self.remote_new_count += 1
            self.remote_new_bytes += int(resp.get("size", 0))
        else:                
            self.remote_update_count += 1
            self.remote_update_bytes += int(resp.get("size", 0))
        self.write_line(operation, file_id, remote_path, local_path)

        return resp

    def create_folder(self, name, parent_id='root'):
        # Override.
        resp = super().create_folder(name, parent_id=parent_id)
        
        operation = "NEW"
        file_id = resp
        remote_path = self.get_remote_path(file_id)
        local_path = self.UNKNOWN_FIELD
        self.remote_new_count += 1
        self.write_line(operation, file_id, remote_path, local_path)

        return resp

    def delete(self, file_id):
        # Override.
        remote_path = self.get_remote_path(file_id)  # Before we delete it ...

        resp = super().delete(file_id)
        
        operation = "DELETE"
        if remote_path is None: remote_path = self.UNKNOWN_FIELD
        local_path = self.UNKNOWN_FIELD
        self.remote_delete_count += 1
        self.write_line(operation, file_id, remote_path, local_path)

        return resp

    def download_file(self, file_id, save_path, filename=None):
        # Override.
        resp = super().download_file(file_id, save_path, filename=filename)

        operation = "DOWNLOAD"
        remote_path = self.get_remote_path(file_id)
        local_path = resp
        self.download_count += 1
        self.downloaded_bytes += os.path.getsize(local_path)
        self.write_line(operation, file_id, remote_path, local_path)

        return resp

    def create_local_folder(self, path):
        # Override.
        if not os.path.exists(path):
            operation = "DOWNLOAD"
            file_id = self.UNKNOWN_FIELD
            remote_path = self.UNKNOWN_FIELD
            local_path = path
            self.download_count += 1
            self.write_line(operation, file_id, remote_path, local_path)
        return super().create_local_folder(path)
