import os

from backuper import downloader
from backuper import googledrive

def dl_folder(google, folder_id, dest_path):
    dl = downloader.DriveDownloader(google, update_db=False)
    q = dl.start_download_queue(n_threads=8)
    for dirpath, dirnames, filenames in google.walk_folder(folder_id, fields="files(id, md5Checksum, name)"):
        path = os.path.join(dest_path, dirpath[0])
        dir_id = dirpath[1]
        # This is really only necessary for empty folders ...
        q.put(downloader.DLQEntry(type="#folder", file_id=dir_id, path=path))
        for file_resp in filenames:
            q.put(downloader.DLQEntry(type="#file", file_id=file_resp['id'], 
                path=path, filename=file_resp['name'], md5sum=file_resp['md5Checksum']))

    dl.wait_for_queue(q)

if __name__ == '__main__':
    dl_folder(googledrive.GoogleDrive(), "0B94xod46LwqkZlVnN2I1VVNCemc", "tests/")