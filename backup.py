#! python3

import tempfile
import logging
import datetime
import concurrent.futures
from contextlib import contextmanager

from pytools.fileutils import *
from .sharedtools import *
from .dropbox import Dropbox
from .googledrive import GoogleDrive

from apiclient.errors import ResumableUploadError, HttpError

from peewee import *
from peewee import OperationalError

from tqdm import tqdm


MAX_THREADS = 6
RETRYABLE_ERRORS = (ResumableUploadError, HttpError)


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


def backup(dropbox=False, google_drive=True, backup_paths=True, blacklist=True, delete_deleted=False, log_structures=False, log=True):
    with Backup(google=google_drive, my_dropbox=dropbox, log=log) as bkup:
        paths = bkup.read_paths_to_backup()

        if blacklist:
            bkup.blacklist_removed_from_gdrive(log=True)

        if delete_deleted:
            bkup.del_removed_from_local(progress=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            if log_structures:
                executor.submit(upload_log_structures, bkup)

            if backup_paths:
                executor.submit(backup_dirs_to_gdrive, bkup, paths['dirs_to_archive'])

        print("\nDONE")


def upload_log_structures(bkup, clean=True):
    paths = bkup.read_paths_to_backup()
    with bkup.temp_dir(clean=clean) as temp_dir_path:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            for path in paths['paths_to_backup']:
                executor.submit(bkup.write_log_structure, save_to=temp_dir_path, path=path)

            for path in paths['dir_only_paths']:
                executor.submit(bkup.write_log_structure, save_to=temp_dir_path, path=path, dirs_only=True)


def backup_dirs_to_gdrive(bkup, paths=None):
    if paths is None:
        paths = bkup.read_paths_to_backup()['dirs_to_archive']

    if MAX_THREADS <= 1:
        for path in paths:
            bkup.to_google_drive(path)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for path in paths:
                executor.submit(bkup.to_google_drive, path)


class Backup:
    def __init__(self, save_to_path=".", my_dropbox=False, google=True, log=False):
        self.config = Config()
        self.my_dropbox = Dropbox(overwrite=True) if my_dropbox else None
        self.google = GoogleDrive() if google else None

        self.blacklisted = [unify_path(path) for path in self.config.get_section_values(self.config['Paths']['blacklisted'])]

        if log:
            os.makedirs("./Logs/", exist_ok=True)
            log_file = create_filename("./Logs/{}.txt".format(get_date(True)))
            self.blacklisted.append(unify_path(log_file))
            logging.basicConfig(filename=log_file,
                                filemode='w',
                                format=u"%(levelname)s:%(asctime)s:%(threadName)s: %(message)s",
                                datefmt="%Y-%b-%d, %a %H:%M:%S",
                                level=logging.INFO)
            console = logging.StreamHandler()
            formatter = logging.Formatter(u"%(message)s")
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
            self.make_folder_structure(path)
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                for file_path in self.get_files_to_sync(path):
                    executor.submit(retry_operation, self.file_to_drive, file_path, error=RETRYABLE_ERRORS)
            self.write_last_backup_date()
        elif self.is_for_sync(path):
            retry_operation(self.file_to_drive, path, self.get_drive_root_folder_id(), error=RETRYABLE_ERRORS)
            self.write_last_backup_date()

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
        entry = unify_path(path)
        parent_id = self.get_parent_folder_id(entry)

        folder_id = self.get_stored_file_id(entry)
        if folder_id is None:
            folder_id = self.google.create_folder(real_case_filename(entry), parent_id=parent_id)
            db_create(DriveArchive, path=entry, drive_id=folder_id, date_modified_on_disk=date_modified(entry))
        else:  # update database metadata (modified on disk date)
            model = DriveArchive.get(DriveArchive.path == entry)
            db_update(model, path=entry, drive_id=folder_id, date_modified_on_disk=date_modified(entry))

        return folder_id

    def file_to_drive(self, path, folder_id=None):
        entry = unify_path(path)
        if folder_id is None:
            folder_id = self.get_parent_folder_id(entry)
        file_id = self.get_stored_file_id(entry)

        resp = self.google.upload_file(path, folder_id=folder_id, file_id=file_id)
        try:
            db_create(DriveArchive, path=entry, drive_id=resp['id'], date_modified_on_disk=date_modified(entry))
        except IntegrityError:
            model = DriveArchive.get(DriveArchive.path == entry)
            db_update(model, path=entry, drive_id=resp['id'], date_modified_on_disk=date_modified(entry))

        return resp['id']

    def get_drive_root_folder_id(self):
        try:
            return self.config['GoogleDrive']['folder_id']
        except KeyError:
            if not self.google.get_file_data_by_name("Backuper"):
                folder_id = self.google.create_folder("Backuper")
            else:
                folder_id = self.google.get_file_data_by_name("Backuper")[0]['id']
            self.config['GoogleDrive']['folder_id'] = folder_id
        return folder_id

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

    def read_paths_to_backup(self):
        all_paths = {}
        for section in self.config['Paths']:
            paths = [unify_path(path) for path in self.config.get_section_values(self.config['Paths'][section])]
            all_paths[section] = paths
        return all_paths

    def is_for_sync(self, path):
        entry = unify_path(path)
        try:
            stored_modified_date = DriveArchive.get(DriveArchive.path == entry).date_modified_on_disk
            # folder already exists in google drive
            return date_modified(entry) > stored_modified_date if not os.path.isdir(entry) else False
        except DriveArchive.DoesNotExist:
            return True

    def get_all_paths_to_sync(self, path):
        for root, dirs, files in walk(path):
            unified_root = unify_path(root)
            if unified_root in self.blacklisted:
                continue
            if self.is_for_sync(unified_root):
                yield unified_root
            for f in files:
                f_path = unify_path(os.path.join(unified_root, f))
                if f_path not in self.blacklisted and self.is_for_sync(f_path):
                    yield f_path

    def get_files_to_sync(self, path):
        for root, dirs, files in walk(path):
            unified_root = unify_path(root)
            if unified_root in self.blacklisted:
                continue
            for f in files:
                f_path = unify_path(os.path.join(unified_root, f))
                if f_path not in self.blacklisted and self.is_for_sync(f_path):
                    yield f_path

    def get_folders_to_sync(self, path):
        for root, dirs, files in walk(path):
            unified_root = unify_path(root)
            if unified_root in self.blacklisted:
                continue
            if self.is_for_sync(unified_root):
                yield unified_root

    def make_folder_structure(self, path):
        print("Making folder structure in Google Drive for {} ...".format(path))
        for folder_path in self.get_folders_to_sync(path):
            self.dir_to_drive(folder_path)

    def write_last_backup_date(self):
        self.config['GoogleDrive']['last_backup_date'] = datetime.datetime.utcnow().isoformat('T') + 'Z'
        self.config.write_to_config()

    def get_last_backup_date(self, archive=False):
        if archive:
            return datetime.datetime.strptime(self.config['GoogleDrive']['last_backup_date'].rsplit('.', 1)[0],
                                             '%Y-%m-%dT%H:%M:%S')
        return self.config['GoogleDrive']['last_backup_date']

    def get_last_change_token(self):
        if not self.config['GoogleDrive']['last_change_token']:
            return self.google.get_start_page_token()
        return int(self.config['GoogleDrive']['last_change_token'])

    def write_last_change_token(self, change_id):
        self.config['GoogleDrive']['last_change_token'] = str(change_id)

    def get_drive_last_removed(self, update_last_change_token=True):
        changes = self.google.get_changes(start_page_token=self.get_last_change_token(),
                                 fields="changes(removed,time,fileId,file(name,modifiedTime,trashed))")

        # result = []
        for change in changes:
            if change['removed'] or change.get('file', {}).get('trashed'):
                # result.append(change['fileId'])
                yield change['fileId']

        if update_last_change_token:
            self.write_last_change_token(self.google.get_start_page_token())

        # return result

    def blacklist_removed_from_gdrive(self, log=False):
        for removed_file_id in self.get_drive_last_removed():
        # removed_from_drive = self.get_drive_last_removed()
            try:
                archive = DriveArchive.select().where(DriveArchive.drive_id == removed_file_id).get()
            except DriveArchive.DoesNotExist:
                archive = None

            if archive:
                if os.path.exists(archive.path):
                    self.config['Paths']['blacklisted'] += archive.path + ';'
                    dynamic_print("Added {} to blacklist".format(archive.path), True)

                archive.delete_instance()

            # for archive in DriveArchive.select().where(DriveArchive.drive_id << removed_from_drive):

        # return DriveArchive.delete().where(DriveArchive.drive_id << removed_from_drive).execute()

    def del_removed_from_local(self, progress=True):
        # paths_to_delete = []  # list of drive_ids
        # for archive in DriveArchive.select().naive().iterator():
        #     if not os.path.exists(archive.path):
        #         paths_to_delete.append(archive.drive_id)

        #         if progress:
        #             dynamic_print("Removing {} from Google Drive".format(archive.path))

        # self.google.batch_delete(paths_to_delete)  # ignore 404 errors
        # DriveArchive.delete().where(DriveArchive.drive_id << paths_to_delete).execute()

        print("Deleting files removed from disk from Google Drive ...")

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

                future_to_archive[future].delete_instance()

    def write_log_structure(self, save_to=".", path=".", dirs_only=False):
        file_name = r"{}\{}".format(save_to, name_from_path(path, ".txt"))
        with open(file_name, "w", encoding="utf8") as f:
            log_structure(path, dirs_only=dirs_only, output=f)

    @contextmanager
    def temp_dir(self, clean=True):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
            path_to_zip = zip_dir(temp_dir)
            self.google.upload(path_to_zip, folder_id=self.get_logs_folder_id())
            if clean:
                remove_file(path_to_zip)

    def rebuild_database(self):
        """
        Rebuild database by removing non-existent files in Google Drive.

        Used for maintenance.
        """
        print("Rebuilding database ...")
        logging.info("rebuild_database()")

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor, \
                        tqdm(total=DriveArchive.select().count()) as pbar:
            futures = {}
            for archive in DriveArchive.select().naive().iterator():
                futures[executor.submit(retry_operation, self.google.exists, archive.drive_id, error=RETRYABLE_ERRORS)] = archive

            for future in concurrent.futures.as_completed(futures):
                pbar.update()

                if not future.result():  # doesn't exist
                    archive = futures[future]
                    if not os.path.exists(archive.path) or unify_path(archive.path) not in self.blacklisted:
                        logging.info("Removed {} from database.".format(archive.path))
                        archive.delete_instance()

    def update_google_drive_metadata(self):
        """
        Change file names in Google Drive to their original local file system counterparts (for Windows).
        """
        print("Updating file names in Google Drive to their original local system counterparts ...")
        logging.info("update_google_drive_metadata()")

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor, \
                        tqdm(total=DriveArchive.select().count()) as pbar:
            futures = []
            for archive in DriveArchive.select().naive().iterator():
                futures.append(executor.submit(retry_operation, self.google.update_metadata, archive.drive_id,
                                               name=real_case_filename(archive.path), error=RETRYABLE_ERRORS))

            for _ in concurrent.futures.as_completed(futures):
                pbar.update()


# TODO: uploading show file being uploaded on same line (\r) and progress for whole process not just for individual files
# TODO: get all files to sync and show progress based on all files left
# TODO: create logger
