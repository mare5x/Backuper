from time import strftime, gmtime
import time
import os
from os.path import join, getsize, getmtime
from contextlib import redirect_stdout
import shutil
import scandir

# TODO implement max_depth
# TODO zip source code files

WHITE_LIST_FILES = (".ui", ".py", ".h", ".cpp", ".txt", ".c", ".exe",
                    ".png", ".PNG", ".jpg", ".jpeg", ".gif", ".pdf", ".avi", ".mp3",
                    ".mp4", ".epub", ".mobi", ".azw3", ".doc", ".docx", ".ppt", ".pptx")
BLACK_LIST_DIRS_PREFIX = (".", "_", ".git")
BLACK_LIST_DIRS_SUFFIX = (".git", ".pdb")


def get_date(for_file=False):
    if for_file:
        return "{}".format(strftime("%Y_%b_%d", gmtime()))
    else:
        return "{}".format(strftime("%Y %b %d, %a %H:%M:%S", gmtime()))


def file_last_modified(path):
    return time.ctime(getmtime(path))


def name_from_path(path, end="", raw=False):
    path = os.path.realpath(path)
    if raw:
        return r"{}".format(path.rsplit("\\", 1)[-1])
    else:
        return r"{}_{}_BACKUP{}".format(get_date(for_file=True), path.rsplit("\\", 1)[-1], end)


def get_structure(path=".", dirs_only=False):
    print("Logging on {}\n".format(get_date()))

    for root, dirs, files in scandir.walk(path):
        root_size = sum(getsize(join(root, name)) for name in files)

        # dirs[:] is by reference, dirs = is assignment
        dirs[:] = [_dir for _dir in dirs if not _dir.startswith(BLACK_LIST_DIRS_PREFIX)
                   and not _dir.endswith(BLACK_LIST_DIRS_SUFFIX)]
        files = [_file for _file in files if _file.endswith(WHITE_LIST_FILES)]
        indent_level = root.count(os.sep) - path.count(os.sep) - 1

        print("{}{} last modified: {}".format(' ' * 4 * indent_level, root, file_last_modified(root)), end='')
        print(", {} used".format(convert_file_size(root_size)))

        if not dirs_only:
            for _file in files:
                print("{}{}".format(' ' * 4 * (indent_level + 1), _file))


def write_backup_file(save_to=".", path=".", get_dirs_only=False):
    file_name = r"{}\{}".format(save_to, name_from_path(path, ".txt"))
    # file_name = r"{}_{}_BACKUP.txt".format(get_date(True), path.rsplit("\\", 1)[-1])
    with open(file_name, "w", encoding="utf8") as f:
        with redirect_stdout(f):
            get_structure(path, dirs_only=get_dirs_only)


def create_dir(path="."):
    dir_name = "{}".format(join(path, get_date(True)))
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    return dir_name


def remove_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)


def remove_file(path):
    if os.path.isfile(path):
        os.remove(path)


def zip_dir(path_to_zip, name=None, save_path=".\\"):
    if name:
        return shutil.make_archive(save_path + name, "zip", path_to_zip)
    return shutil.make_archive(save_path + get_date(for_file=True), "zip", path_to_zip)


def get_time_from_secs(secs):
    """
    Calculate hours, minutes, seconds from seconds.

    Return tuple (h, min, s)
    """
    # divmod = divide and modulo -- divmod(1200 / 1000)  =  (1, 200)
    mins, secs = divmod(secs, 60)
    hours, mins = divmod(mins, 60)
    return "{0:02.0f}:{1:02.0f}:{2:02.0f}".format(hours, mins, secs)


def get_file_size(path):
    return convert_file_size(getsize(path))


def convert_file_size(_bytes):
    """ Return string of appropriate size for given bytes.
    """
    kb = _bytes / 1024

    if (kb / 1024**2) > 1:
        return "{:.2f} GB".format(kb / 1024**2)
    elif (kb / 1024) > 1:
        return "{:.2f} MB".format(kb / 1024)
    else:
        return "{:.2f} KB".format(kb)
