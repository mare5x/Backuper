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
    CONFLICT_FLAG = -1
    NEUTRAL_FLAG = 0
    SAFE_FLAG = 1

    def __init__(self, settings, google):
        self.conf = settings
        self.google = google

    def is_for_download(self, file_id, file_md5, remote_time=None):
        """Check if a file on Google Drive is to be downloaded.

        Returns:
            NEUTRAL_FLAG: int, don't sync
            SAFE_FLAG: int, safe sync
            CONFLICT_FLAG: int, conflict
        Note:
            Manually check if the file_id points to a folder.
        """
        # Ugly case analysis code ahead.

        CONFLICT_FLAG, NEUTRAL_FLAG, SAFE_FLAG = self.CONFLICT_FLAG, self.NEUTRAL_FLAG, self.SAFE_FLAG

        # The first check is whether the file in the cloud is also in the
        # local database. If the file is not in the database, 
        # it can't hurt to download the file.
        db_entry = db.GoogleDriveDB.get("drive_id", file_id)
        if db_entry:
            # It is possible for a file to be in the database, despite being
            # blacklisted. In that case, we do not want to download it.
            if self.conf.is_blacklisted(db_entry.path):
                return NEUTRAL_FLAG

            # The file has been deleted from the local file system. Was that intentional?
            if not os.path.exists(db_entry.path):
                return CONFLICT_FLAG

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
            if p or q: return NEUTRAL_FLAG
            if r: return SAFE_FLAG

            # The conflict might be resolved by looking at the change time ...
            if remote_time is not None and (remote_time < db_entry.date_modified_on_disk \
                                            or remote_time < ft.date_modified(db_entry.path)):
                return NEUTRAL_FLAG
            return CONFLICT_FLAG
        return SAFE_FLAG

    def get_parent_archive(self, file_id):
        parent_archive = None
        for parent_id in self.google.get_parents(file_id):
            parent_archive = db.GoogleDriveDB.get("drive_id", parent_id)
            if parent_archive:
                return parent_archive
        return parent_archive

    _get_changes_to_download_obj = namedtuple("get_changes_to_download_obj", 
        ["sync_decision", "type", "file_id", "remote_path", "md5checksum"])
    def get_changes_to_download(self, root_path=None, update_token=False):
        """Yields changed files/folders descended from the given root path.
        root_path should be a valid remote path.
        Yields: an object with the following fields: "sync_decision", "type", "file_id", "remote_path", "md5checksum"
        sync_decision: SAFE_FLAG: int, safe sync
                       CONFLICT_FLAG: int, conflict.
        type: #folder or #file
        remote path string: e.g. "Backuper\\Folder\\file.py"
        """
        ret_type = self._get_changes_to_download_obj

        if root_path is None:
            root_folder_id = self.conf.get_root_folder_id(self.google)
            root_path = self.google.get_remote_path(root_folder_id)

        last_download_change_token = self.conf.data_file.get_last_download_change_token()
        if last_download_change_token == -1:
            last_download_change_token = self.google.get_start_page_token()

        # Ignore the trees folder ...
        blacklisted_remote_paths = set()
        tmp = self.conf.data_file.get_trees_folder_id()
        if tmp: blacklisted_remote_paths.add(self.google.get_remote_path(tmp))
        
        def is_valid_path(remote_path):
            if not remote_path.startswith(root_path): return False
            for path in blacklisted_remote_paths:
                if remote_path.startswith(path): return False
            return True

        # Note: it is important to check the parent of each changed file because the listed
        # changes are global (from any folder in My Drive). Remote paths are used because
        # they are "fast" and work for un-archived folders. However, multiple folders 
        # can share the same remote path (bad).

        changes = self.google.get_changes(start_page_token=last_download_change_token,
            fields="changes(file(id, name, md5Checksum, modifiedTime, parents, trashed, mimeType))",
            include_removed=False)

        last_download_sync_datetime = self.conf.data_file.get_last_download_sync_time(False)
        for change in changes:
            file_change = change["file"]
            if file_change["trashed"]:
                continue
            change_datetime = googledrive.convert_google_time_to_datetime(file_change['modifiedTime'])
            if change_datetime < last_download_sync_datetime:
                continue
            file_id = file_change["id"]
            remote_path = self.google.get_remote_path(file_id)
            if not is_valid_path(remote_path): 
                continue
            md5sum = file_change.get("md5Checksum", "")
            decision = self.is_for_download(file_id, md5sum, change_datetime)
            if decision != self.NEUTRAL_FLAG:
                _type = "#folder" if (file_change["mimeType"] == self.google.FOLDER_MIMETYPE) else "#file"
                yield ret_type(decision, _type, file_id, remote_path, md5sum)

        if update_token:
            self.conf.data_file.set_last_download_change_token(self.google.get_start_page_token())

    _ids_to_download_in_folder_obj = namedtuple("ids_to_download_in_folder_obj", 
        ["sync_decision", "type", "file_id", "remote_path", "md5checksum"])
    def get_ids_to_download_in_folder(self, folder_id):
        """Only yields outdated, missing or conflict files/folders.
        Yields: an object with the following fields: "sync_decision", "type", "file_id", "remote_path", "md5checksum"
        sync_decision: SAFE_FLAG: int, safe sync
                       CONFLICT_FLAG: int, conflict.
        type: #folder or #file
        remote path string: e.g. "Backuper\\Folder\\file.py"
        """
        ret_type = DriveFileCrawler._ids_to_download_in_folder_obj
        for dirpath, dirnames, filenames in self.google.walk_folder(folder_id, fields="files(id, md5Checksum, name, modifiedTime)"):
            path, file_id = dirpath
            md5sum = db.GoogleDriveDB.FOLDER_MD5
            sync_decision = self.is_for_download(file_id, md5sum)
            if sync_decision != self.NEUTRAL_FLAG:
                yield ret_type(sync_decision, "#folder", file_id, path, md5sum)

            for resp in filenames:
                file_id = resp['id']
                md5sum = resp.get("md5Checksum", db.GoogleDriveDB.FOLDER_MD5)
                change_datetime = googledrive.convert_google_time_to_datetime(resp['modifiedTime'])
                sync_decision = self.is_for_download(file_id, md5sum, change_datetime)

                if sync_decision != self.NEUTRAL_FLAG:                
                    yield ret_type(sync_decision, "#file", file_id, os.path.join(path, resp["name"]), md5sum)

    def get_last_removed(self, update_token=True):
        # NOTE: if a folder is removed, only that folder deletion is reported (not the folder contents)!

        conf = self.conf.data_file

        start_token = conf.get_last_removed_change_token()
        if start_token == -1:
            start_token = self.google.get_start_page_token()

        changes = self.google.get_changes(start_page_token=start_token,
            fields="changes(file(name,trashed),fileId,removed)",
            include_removed=True)

        for change in changes:
            if change['removed'] or change.get('file', {}).get('trashed'):
                yield change['fileId']

        if update_token:
            conf.set_last_removed_change_token(self.google.get_start_page_token())
