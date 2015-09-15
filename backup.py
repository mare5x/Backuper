#! python3

import tempfile
import logging
import datetime
from contextlib import contextmanager

from pytools.fileutils import *
from .sharedtools import *
from .dropbox import Dropbox
from .googledrive import GoogleDrive

from apiclient.errors import ResumableUploadError

from peewee import *
from peewee import OperationalError


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


class Backup:
    def __init__(self, save_to_path=".", my_dropbox=None, google=None, log=False):
        self.config = Config()
        self.my_dropbox = my_dropbox
        self.google = google

        self.blacklisted = [unify_path(path) for path in self.config.get_section_values(self.config['Paths']['blacklisted'])]

        if log:
            os.makedirs("./Logs/", exist_ok=True)
            log_file = create_filename("./Logs/{}.txt".format(get_date(True)))
            self.blacklisted.append(unify_path(log_file))
            logging.basicConfig(filename=log_file,
                                filemode='w',
                                format="%(levelname)s:%(asctime)s:%(threadName)s: %(message)s",
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

    def to_google_drive(self, path, google=None):
        if google is None:
            google = self.google

        if os.path.isdir(path):
            for _path in self.get_paths_to_sync(path):
                if os.path.isdir(_path):
                    retry_operation(self.dir_to_drive, _path, num_retries=5, wait_time=1, error=ResumableUploadError)
                else:
                    retry_operation(self.file_to_drive, _path, num_retries=5, wait_time=1, error=ResumableUploadError)
            self.write_last_backup_date()
        elif self.is_for_sync(path):
            retry_operation(self.file_to_drive, path, self.get_drive_root_folder_id(), num_retries=5, wait_time=1,
                            error=ResumableUploadError)
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

    def dir_to_drive(self, path, google=None):
        if google is None:
            google = self.google

        entry = unify_path(path)
        parent_id = self.get_parent_folder_id(entry)

        new_id = self.get_stored_file_id(entry)
        if new_id is None:
            new_id = google.create_folder(name_from_path(path, raw=True), parent_id=parent_id)
            db_create(DriveArchive, path=entry, drive_id=new_id, date_modified_on_disk=date_modified(entry))
        return new_id

    def file_to_drive(self, path, folder_id=None, google=None):
        if google is None:
            google = self.google

        entry = unify_path(path)
        if folder_id is None:
            folder_id = self.get_parent_folder_id(entry)
        file_id = self.get_stored_file_id(entry)

        resp = google.upload_file(path, folder_id=folder_id, file_id=file_id)
        try:
            db_create(DriveArchive, path=entry, drive_id=resp['id'], date_modified_on_disk=date_modified(entry))
        except IntegrityError:
            model = DriveArchive.get(DriveArchive.path == entry)
            db_update(model, path=entry, drive_id=resp['id'], date_modified_on_disk=date_modified(entry))

        return resp['id']

    def get_drive_root_folder_id(self, google=None):
        if google is None:
            google = self.google

        try:
            return self.config['GoogleDrive']['folder_id']
        except KeyError:
            if not google.get_file_data_by_name("Backuper"):
                folder_id = google.create_folder("Backuper")
            else:
                folder_id = google.get_file_data_by_name("Backuper")[0]['id']
            self.config['GoogleDrive']['folder_id'] = folder_id
        return folder_id

    def get_logs_folder_id(self, google=None):
        if google is None:
            google = self.google

        try:
            return self.config['GoogleDrive']['logs_folder_id']
        except KeyError:
            if google.get_file_data_by_name("Structure logs"):
                folder_id = google.get_file_data_by_name("Structure logs")[0]['id']
            else:
                folder_id = google.create_folder("Structure logs", parent_id=self.get_drive_root_folder_id())
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
            modified_date = DriveArchive.get(DriveArchive.path == entry).date_modified_on_disk
            return date_modified(entry) > modified_date
        except DriveArchive.DoesNotExist:
            return True

    def get_paths_to_sync(self, path):
        for root, dirs, files in walk(path):
            unified_root = unify_path(root)
            if unified_root in self.blacklisted:
                continue
            if self.is_for_sync(unified_root):
                yield root
            for f in files:
                f_path = unify_path(os.path.join(unified_root, f))
                if f_path not in self.blacklisted and self.is_for_sync(f_path):
                    yield os.path.join(root, f)

    def write_last_backup_date(self):
        self.config['GoogleDrive']['last_backup_date'] = datetime.datetime.utcnow().isoformat('T') + 'Z'
        self.config.write_to_config()

    def get_last_backup_date(self, archive=False):
        if archive:
            return datetime.datetime.strptime(self.config['GoogleDrive']['last_backup_date'].rsplit('.', 1)[0],
                                             '%Y-%m-%dT%H:%M:%S')
        return self.config['GoogleDrive']['last_backup_date']

    def get_last_change_id(self):
        return int(self.config['GoogleDrive']['last_change_id'])

    def write_last_change_id(self, change_id):
        self.config['GoogleDrive']['last_change_id'] = str(change_id)

    def get_drive_last_removed(self, update_last_change_id=False, google=None):
        if google is None:
            google = self.google

        changes = google.get_changes(start_change_id=self.get_last_change_id(),
                                 fields="largestChangeId,items(deleted,modificationDate,fileId,file(title,modifiedDate,labels/trashed))")
        result = []
        for change in changes['items']:
            if change['deleted'] or change.get('file', {}).get('labels', {}).get('trashed'):
                result.append(change['fileId'])

        if update_last_change_id:
            self.write_last_change_id(changes.get('largestChangeId', self.get_last_change_id()))
        return result

# for i in g.drive_service.changes().list(includeSubscribed=False, maxResults=1000).execute()['items']:
#     if datetime.datetime.strptime(i['modificationDate'].rsplit('.')[0], '%Y-%m-%dT%H:%M:%S') > datetime.datetime(2015, 9, 4):
#         print(i)
# g.drive_service.changes().list(includeSubscribed=False, maxResults=1000, pageToken=285800, fields="items(deleted,modificationDate,fileId,file(title,modifiedDate,labels/trashed))").execute()['items']
    def del_removed_from_local(self, log=False, google=None):
        if google is None:
            google = self.google

        deleted = 0
        for archive in DriveArchive.select():
            if not os.path.exists(archive.path) and google.exists(archive.drive_id):
                google.delete(archive.drive_id)

                if log:
                    dynamic_print("Removing {} from Google Drive".format(archive.path))

                deleted += DriveArchive.delete_instance(archive)
        return deleted

    def del_removed_from_drive(self, log=False):
        # current = {item['id'] for item in google.drive_service.files().list(
        #            q="modifiedDate > '{}'".format(self.get_last_backup_date())).execute()['items']}

        # current = {drive_id['id'] for drive_id in google.list_all(fields="items/id")}
        # archived = {archive.drive_id for archive in DriveArchive.select(DriveArchive.drive_id)}
        # archived = {archive.drive_id for archive in DriveArchive.select(DriveArchive.drive_id).where(
        #                                             DriveArchive.date_modified_on_disk > self.get_last_backup_date(True))}
        # removed_from_drive = list(archived - current)

        removed_from_drive = self.get_drive_last_removed(update_last_change_id=True)

        for archive in DriveArchive.select().where(DriveArchive.drive_id << removed_from_drive):
            self.config['Paths']['blacklisted'] += archive.path + ';'

            dynamic_print("Added {} to blacklist".format(archive.path))
        return DriveArchive.delete().where(DriveArchive.drive_id << removed_from_drive).execute()

    def write_log_structure(self, save_to=".", path=".", dirs_only=False):
        file_name = r"{}\{}".format(save_to, name_from_path(path, ".txt"))
        with open(file_name, "w", encoding="utf8") as f:
            log_structure(path, dirs_only=dirs_only, output=f)

    @contextmanager
    def temp_dir(self, clean=True, google=None):
        if google is None:
            google = self.google

        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
            path_to_zip = zip_dir(temp_dir)
            google.upload(path_to_zip, folder_id=self.get_logs_folder_id())
            if clean:
                remove_file(path_to_zip)

# TODO: uploading show file being uploaded on same line (\r) and progress for whole process not just for individual files
# TODO: get all files to sync and show progress based on all files left
# TODO: create logger
