import os
import tempfile
import time

from pytools import printer, filetools

from backuper import googledrive


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

def test_file_upload(pretty=False):
    # Upload an empty file. Update it. Upload it. Empty it. Upload it.
    FPATH = "tests/test.txt"
    FIELDS = "createdTime,id,md5Checksum,mimeType,modifiedByMe,modifiedByMeTime,modifiedTime,name,parents,quotaBytesUsed,size,version"
    PRETTY_FPATH = "tests/test_file_upload.log"

    if pretty:
        fpretty = open(PRETTY_FPATH, "w")
        g = googledrive.PPGoogleDrive(fpretty)

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

    if pretty:
        fpretty.close()
        print(PRETTY_FPATH)

def test_list():
    for r in g.get_files_in_folder('root'):
        print(r)
    for r in g.get_folders_in_folder('root'):
        print(r)

def test_walk_folder(folder_id):
    for dirpath, dirnames, filenames in g.walk_folder(folder_id, fields="files(id, md5Checksum, name)"):
        print(dirpath, dirnames, filenames)

def test_pretty_print():
    with open("tests/test.txt", "w") as f:
        googledrive.PPGoogleDrive.SECTION_WIDTHS = [4, 4, 21, 20]        
        pp = googledrive.PPGoogleDrive(f)
        pp.write_line("A" * 9, "B" * 18, "C" * 21, "D" * 42)
        pp.exit()

def test_pretty_full():
    LOG_PATH = "tests/test_pretty_full.log"
    with open(LOG_PATH, "w") as f:
        pp = googledrive.PPGoogleDrive(f)  
        # folder_id = pp.upload_directory("tests/")
        
        folder_id = ''
        
        with tempfile.TemporaryDirectory() as tmpdir:
            folder_id = pp.create_folder("test folder")
            file1 = os.path.join(tmpdir, "file1.txt")
            f = open(file1, "w")
            f.write("hello")
            f.close()
            file_id = pp.upload_file(file1, folder_id=folder_id)['id']
            f = open(file1, "a")
            f.write(", world!")
            f.close()
            pp.upload_file(file1, folder_id=folder_id, file_id=file_id)
            pp.delete(file_id)
            pp.upload_file(file1, folder_id=folder_id)
        
            pp.download_folder(folder_id, tmpdir)

        pp.delete(folder_id)
        pp.exit()

        print("Remote path cache: hits: {}, misses: {}".format(pp.remote_cache.hits, pp.remote_cache.misses))
    print(LOG_PATH)

if __name__ == "__main__":
    # test_progress_bar()

    # g.upload_directory("backuper")
    # g.download_file('1mLmwd_FuxmyKMRLcGWVF8xGumbCSPvu4', "tests/")
    # g.download_folder('0B94xod46LwqkZlVnN2I1VVNCemc', "tests/")

    # test_changes()
    # test_file_upload()
    # test_list()
    # test_walk_folder("0B94xod46LwqkSVIyTktCMVV1QWM")
    # test_pretty_print()
    # test_file_upload(True)
    test_pretty_full()