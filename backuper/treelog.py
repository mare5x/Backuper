import os
import logging
import tempfile

from pytools import filetools as ft


TREE_LOGS_FOLDER_NAME = "BackuperTreeLogs"


def create_tree_log(path_to_log, dst_dir, file_name=None, files=False):
    logging.info("TREE LOG: Starting %s.", path_to_log)
    print("TREE: {} ......... ".format(path_to_log), end='', flush=True)

    if file_name is None:
        file_name = "{}_{}.log".format(ft.path_filter(path_to_log), ft.get_current_date_string())
    
    log_file_path = os.path.join(dst_dir, file_name)
    with open(log_file_path, "w", encoding="utf8") as f:
        ft.tree(path_to_log, files=files, stream=f)

    print("{} DONE.".format(file_name), flush=True)
    logging.info("TREE LOG: Finished %s.", path_to_log)
    
    return log_file_path

def create_tree_logs(conf, dst_dir):
    user_settings = conf.user_settings_file
    for path in user_settings.get_paths_in_option("tree_with_files"):
        create_tree_log(path, dst_dir, files=True)
    for path in user_settings.get_paths_in_option("tree_dirs"):
        create_tree_log(path, dst_dir, files=False)

def create_tree_logs_zip(conf, dst_dir):
    with tempfile.TemporaryDirectory() as tmpdir:
        create_tree_logs(conf, tmpdir)
        zip_name = "TreeLogs{}".format(ft.get_current_date_string())
        return ft.zip_dir(tmpdir, dst_filename=zip_name, dst_dir=dst_dir)

def get_or_create_tree_folder_id(conf, google, root_id):
    tree_folder_id = conf.data_file.get_trees_folder_id()
    if tree_folder_id is None:
        tree_folder_id = google.create_folder(TREE_LOGS_FOLDER_NAME, parent_id=root_id)
        conf.data_file.set_trees_folder_id(tree_folder_id)
    return tree_folder_id
