from backuper import googledrive
from pytools import printer

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

if __name__ == "__main__":
    # test_progress_bar()

    # g.upload_directory("backuper")
    # g.download_file('1mLmwd_FuxmyKMRLcGWVF8xGumbCSPvu4', "tests/")
    # g.download_folder('0B94xod46LwqkZlVnN2I1VVNCemc', "tests/")