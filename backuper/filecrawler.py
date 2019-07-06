import os
from collections import namedtuple

from pytools import filetools as ft

from . import database as db
from . import googledrive


class LocalFileCrawler:
    def __init__(self, settings):
        self.conf = settings

    def is_for_sync(self, path):
        """Note: make sure path is not blacklisted."""
        entry = db.unify_path(path)
        archive = db.GoogleDriveDB.get("path", entry)
        if archive is not None:
            # Folder already exists in google drive.
            return ft.date_modified(entry) > archive.date_modified_on_disk if not os.path.isdir(entry) else False
        return True

    def get_all_paths_to_sync(self, path):
        for root, dirs, files in os.walk(path):
            if self.conf.is_blacklisted(root):
                dirs.clear()
                continue
            if self.is_for_sync(root):
                yield root
            for f in files:
                f_path = os.path.join(root, f)
                if not self.conf.is_blacklisted(f_path) and self.is_for_sync(f_path):
                    yield f_path

    def get_files_to_sync(self, path):
        for root, dirs, files in os.walk(path):
            if self.conf.is_blacklisted(root):
                dirs.clear()
                continue
            for f in files:
                f_path = os.path.join(root, f)
                if not self.conf.is_blacklisted(f_path) and self.is_for_sync(f_path):
                    yield f_path

    def get_folders_to_sync(self, path):
        for root, dirs, files in os.walk(path):
            if self.conf.is_blacklisted(root):
                dirs.clear()
                continue
            if self.is_for_sync(root):
                yield root


class DriveFileCrawler:
    def __init__(self, settings, google):
        self.conf = settings
        self.google = google

    def is_for_download(self, file_id, file_md5):
        """Check if a file on Google Drive is to be downloaded.

        Returns:
            0: int, don't sync
            1: int, safe sync
            -1: int, conflict
        Note:
            Manually check if the file_id points to a folder.
        """
        # TODO check time modified?

        # Ugly case analysis code ahead.

        # The first check is whether the file in the cloud is also in the
        # local database. If the file is not in the database, 
        # it can't hurt to download the file.
        db_entry = db.GoogleDriveDB.get("drive_id", file_id)
        if db_entry:
            # It is possible for a file to be in the database, despite being
            # blacklisted. In that case, we do not want to download it.
            if self.conf.is_blacklisted(db_entry.path):
                return 0

            # The file has been deleted from the local file system. Was that intentional?
            if not os.path.exists(db_entry.path):
                return -1

            # We use md5 checksums because Google provides them when requesting files.
            # There are 3 different md5 checksums we can check: the local file, the 
            # one stored in the database and the one provided by Google.
            # Note: db_entry.md5sum denotes the hash of the file of when it was uploaded 
            # to the remote.
            
            # Truth table for md5 comparison:
            # p := (remote_md5 ~ db_md5)
            # q := (remote_md5 ~ local_md5)
            # r := (local_md5 ~ db_md5)
            # 
            # p q r | f
            # ----------
            # 0 0 0 |-1  conflict (all 3 different checksums differ, local and remote change)
            # 0 0 1 | 1  remote ahead
            # 0 1 0 | 0  no change (database behind)
            # 0 1 1 | 0  impossible due to transitivity
            # 1 0 0 | 0  local ahead
            # 1 0 1 | 0  impossible due to transitivity
            # 1 1 0 | 0  impossible due to transitivity
            # 1 1 1 | 0  no change
            # 
            # f_{0}  := p or q
            # f_{1}  := not(p) and not(q) and r
            # f_{-1} := not(p) and not(q) and not(r)

            local_md5 = ft.md5sum(db_entry.path)
            p = file_md5 == db_entry.md5sum
            q = file_md5 == local_md5
            r = local_md5 == db_entry.md5sum
            if p or q: return 0
            if r: return 1
            return -1
        return 1

    def get_parent_archive(self, file_id):
        parent_archive = None
        for parent_id in self.google.get_parents(file_id):
            parent_archive = db.GoogleDriveDB.get("drive_id", parent_id)
            if parent_archive:
                return parent_archive
        return parent_archive

    def get_changes_to_download(self):
        """Yields changed files/folders descended from an archived directory.
        Yields: (int: sync decision, str: file id, str: name, str: md5Checksum)
            1: int, safe sync
            -1: int, conflict
        """
        last_download_change_token = self.conf.data_file.get_last_download_change_token()
        if last_download_change_token == -1:
            last_download_change_token = self.google.get_start_page_token()

        # Note: it is important to check the parent of each changed file because the listed
        # changes are global (from any folder in My Drive). We do parent checking using the
        # local database because it is much faster than querying the API.

        changes = self.google.get_changes(start_page_token=last_download_change_token,
            fields="changes(file(id, name, md5Checksum, modifiedTime, parents))",
            include_removed=False)

        last_download_sync_datetime = self.conf.data_file.get_last_download_sync_time(False)
        for change in changes:
            file_change = change["file"]
            change_datetime = googledrive.convert_google_time_to_datetime(file_change['modifiedTime'])
            parent_id = file_change["parents"][0]  # Assume single parent.
            if change_datetime < last_download_sync_datetime or self.get_parent_archive(parent_id) is None:
                continue

            file_id = file_change["id"]
            md5sum = file_change.get("md5Checksum", "")
            decision = self.is_for_download(file_id, md5sum)
            if decision != 0:
                yield decision, file_id, file_change['name'], md5sum

    _ids_to_download_in_folder_obj = namedtuple("ids_to_download_in_folder_obj", 
        ["sync_decision", "type", "file_id", "remote_path", "md5checksum"])
    def get_ids_to_download_in_folder(self, folder_id):
        """ Only yields outdated, missing or conflict files/folders.
        Yields: an object with the following fields: "sync_decision", "type", "file_id", "remote_path", "md5checksum"
        sync_decision: 1: int, safe sync
                       -1: int, conflict.
        type: #folder or #file
        remote path string: e.g. "Backuper\\Folder\\file.py"
        """
        for file_type, remote_path, response in self.google.walk_folder_builder(folder_id, "", fields="files(id, md5Checksum, name)"):
            file_id = response['id']
            md5sum = response.get("md5Checksum", googledrive.GoogleDrive.FOLDER_MIMETYPE)
            sync_decision = self.is_for_download(file_id, md5sum)

            if sync_decision != 0:
                if file_type == "#file":
                    remote_path = os.path.join(remote_path, response['name'])
                
                yield DriveFileCrawler._ids_to_download_in_folder_obj(sync_decision, file_type, file_id, remote_path, md5sum)
