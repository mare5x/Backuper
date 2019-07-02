import tempfile
import logging
import datetime
import concurrent.futures
from contextlib import contextmanager

from .sharedtools import *

from pytools import filetools as ft

from .dropbox import Dropbox
from .googledrive import GoogleDrive

from apiclient.errors import ResumableUploadError, HttpError

from tqdm import tqdm


MAX_THREADS = 6
RETRYABLE_ERRORS = (ResumableUploadError, HttpError)


def upload_log_structures(bkup, clean=True):
    with bkup.temp_dir(clean=clean) as temp_dir_path:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            for path in bkup.get_config_path('log_paths_full'):
                executor.submit(bkup.write_log_structure, save_to=temp_dir_path, path=path)

            for path in bkup.get_config_path('log_dirs_only'):
                executor.submit(bkup.write_log_structure, save_to=temp_dir_path, path=path, dirs_only=True)


def google_drive_sync(bkup, backup_sync, download_sync, paths=None):
    if paths is None:
        paths = bkup.get_config_path('sync_dirs')

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # first backup sync a path and once that is done download sync it
        futures = {}
        for path in paths:
            if backup_sync:
                futures[executor.submit(bkup.to_google_drive, path)] = path
            elif download_sync:
                executor.submit(bkup.download_sync_path, path)

        for future in concurrent.futures.as_completed(futures):
            if download_sync:
                executor.submit(bkup.download_sync_path, futures[future])

            del futures[future]


def backup_sync_to_gdrive(bkup, paths=None):
    if paths is None:
        paths = bkup.get_config_path('sync_dirs')

    if MAX_THREADS <= 1:
        for path in paths:
            bkup.to_google_drive(path)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for path in paths:
                executor.submit(bkup.to_google_drive, path)


class Backup:
    def __init__(self, my_dropbox=False, google=True, log=False):
        self.config = Config()
        self.my_dropbox = Dropbox(overwrite=True) if my_dropbox else None
        self.google = GoogleDrive() if google else None

        # blacklisted paths, folder names and file extensions are excluded and so are all
        # the children of those paths/folders
        # blacklisted_extensions work for both folders and files
        self.blacklisted_paths = {unify_path(path) for path in self.config.get_section_values(self.config['Backuper']['blacklisted_paths'])}
        self.blacklisted_extensions = {unify_str(ext) for ext in self.config.get_section_values(self.config['Settings']['blacklisted_extensions'])}
        self.blacklisted_names = {unify_str(name) for name in self.config.get_section_values(self.config['Settings']['blacklisted_names'])}
        user_blacklist = self.get_config_path("blacklisted")
        self.blacklisted_paths.update(user_blacklist)

        self.config_sync_dirs = self.get_config_path("sync_dirs")

        if log:
            os.makedirs("./Logs/", exist_ok=True)
            log_file = ft.create_filename("./Logs/{}.txt".format(ft.get_date(True)))
            self.blacklisted_paths.add(unify_path(log_file))
            logging.basicConfig(filename=log_file,
                                filemode='w',
                                format=u"%(levelname)s:%(asctime)s:%(threadName)s: %(message)s",
                                datefmt="%Y-%b-%d, %a %H:%M:%S",
                                level=logging.INFO)
            console = logging.StreamHandler()
            formatter = logging.Formatter(u"%(message)s")
            console.setFormatter(formatter)
            logging.getLogger('backuper').addHandler(console)

        db.connect()
        db.create_tables([DriveArchive, DropboxArchive], True)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        db.close()
        self.config.write_to_config()

    def to_dropbox(self, path):
        self.my_dropbox.upload_file(path, file_name="{}.zip".format(ft.get_date(for_file=True)))

    def to_google_drive(self, path):
        if self.is_blacklisted(path):
            return

        if os.path.isdir(path):
            self.make_folder_structure(path)
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                for file_path in self.get_files_to_sync(path):
                    executor.submit(retry_operation, self.file_to_drive, file_path, error=RETRYABLE_ERRORS)
            self.write_last_backup_date()
        elif self.is_for_sync(path):
            retry_operation(self.file_to_drive, path, self.get_drive_root_folder_id(), error=RETRYABLE_ERRORS)
            self.write_last_backup_date()

    def get_logs_folder_id(self):
        try:
            return self.config['GoogleDrive']['logs_folder_id']
        except KeyError:
            if self.google.get_file_data_by_name("Structure logs"):
                folder_id = self.google.get_file_data_by_name("Structure logs")[0]['id']
            else:
                folder_id = self.google.create_folder("Structure logs", parent_id=self.get_drive_root_folder_id())
            self.config['GoogleDrive']['logs_folder_id'] = folder_id
        return folder_id

    def is_for_download(self, file_id, md5checksum):
        """Check if a file on Google Drive is to be downloaded.

        Returns:
            0: int, don't sync
            1: int, safe sync
            -1: int, conflict
        Note:
            Manually check if the file_id points to a folder.
        """
        # TODO check time modified?
        model = db_get(DriveArchive, DriveArchive.drive_id, file_id)
        if model:
            if self.is_blacklisted(model.path):
                return 0

            cur_md5sum = ft.md5sum(model.path)
            if cur_md5sum == md5checksum:
                return 0

            if model.md5sum != md5checksum:  # backup sync is behind
                if model.md5sum == cur_md5sum:  # local file is the same as archived (file in cloud is ahead)
                    return 1
                return -1  # local file is different than archived file and different than Google Drive file
            return 0
        return 1

    def get_changes_to_download(self):
        """
        Returns: (int: sync decision, str: file id, str: name, str: md5Checksum)
            2: int, folder
            1: int, safe sync
            -1: int, conflict
        """
        changes = self.google.get_changes(start_page_token=self.get_last_download_change_token(),
                                          fields="changes(file(id, name, mimeType, md5Checksum, modifiedTime))",
                                          include_removed=False)
        for change in changes:
            if self.convert_time_to_datetime(change['modifiedTime']) > self.get_last_download_sync_time(raw=False):
                if change['file']['mimeType'] == 'application/vnd.google-apps.folder':
                    yield 2, change['file']['id'], change['file']['name'], ''
                else:
                    decision = self.is_for_download(change['file']['id'], change.get('md5Checksum', ''))
                    if decision != 0:
                        yield decision, change['file']['id'], change['file']['name'], change.get('md5Checksum', '')

        self.write_last_download_change_token(self.google.get_start_page_token())

    def download_sync_folder(self, folder_id, save_path, overwrite=False, q=None):
        """Sync (download) a Google Drive folder to local drive.

        Positional arguments:
            folder_id: str, Google Drive id of folder
            save_path: str, the root directory of where to download
        Keyword arguments:
            overwrite: bool, whether to overwrite new file if it already exists (default False)
            q: str, query to be used when requesting the Google Drive API (default None)
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = {}
            folder_name = ft.real_case_filename(save_path)
            for file_kind, download_root_path, response in self.google.walk_folder_builder(folder_id, ft.parent_dir(save_path),
                                                                                           folder_name=folder_name,
                                                                                           fields="files(id, md5Checksum, name)",
                                                                                           q=q):
                download_root_path = unify_path(download_root_path)
                if file_kind == "#folder":
                    os.makedirs(download_root_path, exist_ok=True)
                    db_create_or_update(DriveArchive, path=download_root_path, drive_id=response,
                                        date_modified_on_disk=ft.date_modified(download_root_path), md5sum='')
                    continue

                sync_decision = self.is_for_download(response['id'], response['md5Checksum'])
                if sync_decision != 0:  # a file
                    download_path = os.path.join(download_root_path, response['name'])
                    if sync_decision == -1:  # conflict
                        if not overwrite:
                            download_path = ft.create_filename(download_path)

                    futures[executor.submit(retry_operation, self.google.download_file,
                                                             response['id'], *os.path.split(download_path))] = response['id'], response['md5Checksum']  # split download_path into dirname and basename

            with tqdm(total=len(futures)) as pbar:
                for future in concurrent.futures.as_completed(futures):
                    pbar.update()
                    download_path = future.result()
                    if download_path:
                        logging.info("downloaded: {}".format(download_path))
                        db_create_or_update(DriveArchive, path=unify_path(download_path), drive_id=futures[future][0],
                                            date_modified_on_disk=ft.date_modified(download_path), md5sum=futures[future][1])
                    del futures[future]  # free RAM

    def download_sync_changes(self, overwrite=False):
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = {}
            for sync_decision, file_id, name, md5 in self.get_changes_to_download():
                parent_archive = None
                for parent_id in self.google.get_parents(file_id):
                    parent_archive = db_get(DriveArchive, DriveArchive.drive_id, parent_id)
                    if parent_archive:
                        break

                if parent_archive:
                    download_root_path = parent_archive.path
                else:
                    download_root_path = self.config['Paths']['default_download_path']

                download_path = os.path.join(download_root_path, name)

                if sync_decision == 2:
                    os.makedirs(download_path, exist_ok=True)
                    db_create_or_update(DriveArchive, path=download_path, drive_id=file_id,
                                        date_modified_on_disk=ft.date_modified(download_path), md5sum='')
                    continue

                if sync_decision == -1:
                    if not overwrite:
                        download_path = ft.create_filename(download_path)

                futures[executor.submit(retry_operation, self.google.download_file,
                                        file_id, *os.path.split(download_path))] = file_id, md5  # split download_path into dirname and basename

            with tqdm(total=len(futures)) as pbar:
                for future in concurrent.futures.as_completed(futures):
                    pbar.update()
                    download_path = future.result()
                    if download_path:
                        logging.info("downloaded: {}".format(download_path))
                        db_create_or_update(DriveArchive, path=unify_path(download_path), drive_id=futures[future][0],
                                            date_modified_on_disk=ft.date_modified(download_path), md5sum=futures[future][1])
                    del futures[future]  # free RAM

        # for download only folder make new config dir

    def download_sync_path(self, path):
        """Downloads all new or modified files from an already existing backup sync location (arg: path)."""

        path = unify_path(path)
        model = db_get(DriveArchive, DriveArchive.path, path)
        if model:  # exists
            print("\rStarting download sync for: {} ...".format(path))

            if not self.config['GoogleDrive']['last_download_sync_time']:  # first sync -> check all
                self.download_sync_folder(model.drive_id, path, overwrite=False)
            else:
                # don't do this!!! (for highly nested folders)
                self.download_sync_folder(model.drive_id, path, overwrite=False,
                                          q="modifiedTime > '{last_sync_time}'".format(
                                                              last_sync_time=self.get_last_download_sync_time()))

            self.write_last_download_sync_time()
        else:
            logging.error("download_sync({}) model doesn't exist".format(path))

    def make_folder_structure(self, path):
        #logging.info("Making folder structure in Google Drive for {} ...".format(path))
        for folder_path in self.get_folders_to_sync(path):
            self.dir_to_drive(folder_path)

    def get_drive_last_removed(self, update_last_change_token=True):
        changes = self.google.get_changes(start_page_token=self.get_last_change_token(),
            fields="changes(removed,time,fileId,file(name,modifiedTime,trashed))",
            include_removed=True)

        for change in changes:
            if change['removed'] or change.get('file', {}).get('trashed'):
                yield change['fileId']

        if update_last_change_token:
            self.write_last_change_token(self.google.get_start_page_token())

    def del_removed_from_local(self, progress=True):
        """ Delete files removed from disk from Google Drive and the database. """
        # paths_to_delete = []  # list of drive_ids
        # for archive in DriveArchive.select().naive().iterator():
        #     if not os.path.exists(archive.path):
        #         paths_to_delete.append(archive.drive_id)

        #         if progress:
        #             dynamic_print("Removing {} from Google Drive".format(archive.path))

        # self.google.batch_delete(paths_to_delete)  # ignore 404 errors
        # DriveArchive.delete().where(DriveArchive.drive_id << paths_to_delete).execute()

        print("\rDeleting files removed from disk from Google Drive ...")

        files_to_delete = 0
        for archive in DriveArchive.select().naive().iterator():
            if not os.path.exists(archive.path):
                files_to_delete += 1

        # 3 threads so google doesn't cry
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor, tqdm(total=files_to_delete) as pbar:
            future_to_archive = {}
            for archive in DriveArchive.select().naive().iterator():
                if not os.path.exists(archive.path):
                    future_to_archive[executor.submit(self.google.delete, archive.drive_id)] = archive

            for future in concurrent.futures.as_completed(future_to_archive):
                if progress:
                    pbar.update()

                archive = future_to_archive[future]
                logging.info("Removed {} ({}) from database and/or Google Drive.".format(archive.drive_id, archive.path))
                archive.delete_instance()
                del future_to_archive[future]  # no need to store it

    def write_log_structure(self, save_to=".", path=".", dirs_only=False):
        logging.info('Logging structure of {}.'.format(path))

        file_name = r"{}\{}_{}.txt".format(save_to, ft.get_date(for_file=True), ft.name_from_path(path))
        with open(file_name, "w", encoding="utf8") as f:
            ft.tree(path, files=(not dirs_only), stream=f)
        
        logging.info('Finished logging {}.'.format(path))

    @contextmanager
    def temp_dir(self, clean=True):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
            path_to_zip = ft.zip_dir(temp_dir)
            self.google.upload(path_to_zip, folder_id=self.get_logs_folder_id())
            if clean:
                ft.remove_file(path_to_zip)

    def blacklist_removed_from_gdrive(self, log=False):
        for removed_file_id in self.get_drive_last_removed():
            archive = db_get(DriveArchive, DriveArchive.drive_id, removed_file_id, None)
            if archive:
                self.blacklist_path(archive.path, log)
                archive.delete_instance()
        self.clean_blacklisted_paths()

    def remove_blacklisted_paths(self):
        """ Removes archived blacklisted paths from Google Drive and the archive. """
        
        print("\rDeleting blacklisted files from Google Drive ...")

        files_to_delete = 0
        for archive in DriveArchive.select().naive().iterator():
            if self.is_blacklisted_parent(archive.path, self.config_sync_dirs):
                files_to_delete += 1

        # 3 threads so google doesn't cry
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor, tqdm(total=files_to_delete) as pbar:
            future_to_archive = {}
            for archive in DriveArchive.select().naive().iterator():
                if self.is_blacklisted_parent(archive.path, self.config_sync_dirs):
                    future_to_archive[executor.submit(self.google.delete, archive.drive_id)] = archive

            for future in concurrent.futures.as_completed(future_to_archive):
                pbar.update()

                archive = future_to_archive[future]
                logging.info("Removed {} ({}) from database and/or Google Drive.".format(archive.drive_id, archive.path))
                archive.delete_instance()
                del future_to_archive[future]  # no need to store it

    def update_google_drive_metadata(self):
        """Change file names in Google Drive to their original local file system counterparts (for Windows)."""

        print("Updating file names in Google Drive to their original local system counterparts ...")
        logging.info("update_google_drive_metadata()")

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor, \
                        tqdm(total=DriveArchive.select().count()) as pbar:
            futures = []
            for archive in DriveArchive.select().naive().iterator():
                futures.append(executor.submit(retry_operation, self.google.update_metadata, archive.drive_id,
                                               name=ft.real_case_filename(archive.path), error=RETRYABLE_ERRORS))

            for _ in concurrent.futures.as_completed(futures):
                pbar.update()
                del futures[future]
