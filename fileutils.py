# DEPRECATED -- IN PYTOOLS.FILEUTILS!

from datetime import datetime as dtime
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


def date_modified(path, pretty=False, walk=False):
    if pretty:
        return time.ctime(getmtime(path))
    elif walk:
        date = dtime.min
        for root, dirs, files in scandir.walk(path):
            if date_modified(root) > date:
                date = date_modified(root)
        return date
    else:
        return dtime.fromtimestamp(getmtime(path))


def name_from_path(path, end="", raw=False):
    path = os.path.realpath(path)
    if raw:
        return os.path.basename(path)
    else:
        return r"{}_{}_BACKUP{}".format(get_date(for_file=True), os.path.basename(path), end)


def log_structure(path=".", dirs_only=False):
    print("Logging on {}\n".format(get_date()))

    for root, dirs, files in scandir.walk(path):
        try:
            root_size = sum(getsize(join(root, name)) for name in files)
        except PermissionError:
            pass

        # dirs[:] is by reference, dirs = is assignment
        dirs[:] = [_dir for _dir in dirs if not _dir.startswith(BLACK_LIST_DIRS_PREFIX)
                   and not _dir.endswith(BLACK_LIST_DIRS_SUFFIX)]
        files = [_file for _file in files if _file.endswith(WHITE_LIST_FILES)]
        indent_depth = root.count(os.sep) - path.count(os.sep) - 1

        print("{}{} last modified: {}".format(' ' * 4 * indent_depth, root, date_modified(root, pretty=True)), end='')
        print(", {} used".format(convert_file_size(root_size)))

        if not dirs_only:
            for _file in files:
                print("{}{}".format(' ' * 4 * (indent_depth + 1), _file))


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


def parent_dir(path, rel=False):
    if rel:
        return os.path.relpath(os.path.join(path, os.pardir))
    return os.path.abspath(os.path.join(path, os.pardir))


def create_filename(full_path):
    """ c:/users/asdfasf/asdf.exe -> c:/users/asdfasf/asdf (1).exe
        asdf.exe -> asdf (1).exe
    """

    l_path, r_path = os.path.split(full_path)
    filename, extension = os.path.splitext(r_path)
    if not os.path.exists(full_path):
        return os.path.abspath(full_path)

    index = 1
    new_path = os.path.abspath(os.path.join(l_path, "{} ({}){}".format(filename, index, extension)))
    while os.path.exists(new_path):
        index += 1
        new_path = os.path.abspath(os.path.join(l_path, "{} ({}){}".format(filename, index, extension)))

    return new_path
