from backuper import googledrive
from pytools import printer, filetools

import time

g = googledrive.GoogleDrive()

def test_progress_bar():
    n = 100
    t0 = time.time()
    with printer.block() as b:
        for i in range(n):
            g.print_progress_bar(b, i / n, t0, desc="Test:")
            time.sleep(0.1)
        g.print_progress_bar(b, 1, t0, desc="Test:")

def test_changes():
    # token = 820841
    token = g.get_start_page_token()

    folder_id = g.upload_directory("tests/")

    changes = g.get_changes(start_page_token=token,
        fields="changes(file(id, name, mimeType, md5Checksum, modifiedTime))",
        include_removed=False)
    
    for change in changes:
        print(change)

    g.delete(folder_id)

def test_file_upload():
    # Upload an empty file. Update it. Upload it. Empty it. Upload it.
    FPATH = "tests/test.txt"
    FIELDS = "createdTime,id,md5Checksum,mimeType,modifiedByMe,modifiedByMeTime,modifiedTime,name,parents,quotaBytesUsed,size,version"
    
    filetools.create_empty_file(FPATH)
    r = g.upload_file(FPATH, folder_id='root', fields=FIELDS)
    print(r)
    input("Press to continue ...")

    with open(FPATH, "w") as f:
        f.write("How many bytes?")
    r = g.upload_file(FPATH, folder_id='root', file_id=r['id'], fields=FIELDS)
    print(r)
    input("Press to continue ...")
    
    filetools.create_empty_file(FPATH)
    r = g.upload_file(FPATH, folder_id='root', file_id=r['id'], fields=FIELDS)
    print(r)
    input("Press to continue ...")

    g.delete(r['id'])

def test_list():
    for r in g.get_files_in_folder('root'):
        print(r)
    for r in g.get_folders_in_folder('root'):
        print(r)

def test_walk_folder(folder_id):
    for dirpath, dirnames, filenames in g.walk_folder(folder_id, fields="files(id, md5Checksum, name)"):
        print(dirpath, dirnames, filenames)

if __name__ == "__main__":
    # test_progress_bar()

    # g.upload_directory("backuper")
    # g.download_file('1mLmwd_FuxmyKMRLcGWVF8xGumbCSPvu4', "tests/")
    # g.download_folder('0B94xod46LwqkZlVnN2I1VVNCemc', "tests/")

    # test_changes()
    test_file_upload()
    # test_list()
    # test_walk_folder("0B94xod46LwqkSVIyTktCMVV1QWM")