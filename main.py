import argparse
import logging

from pytools import filetools as ft

from backuper import backuper


def main(log=True):
    if log:
        # One log file for each day. Running the program multiple times
        # a day will append to the same file.
        name = "Backuper_{}.log".format(ft.get_current_date_string())
        ft.init_log_file(name, overwrite=True, mode="a")

    parser = argparse.ArgumentParser()
    parser.add_argument("-lsu", action="store_true", help="List changes that will get uploaded (dry run).")
    parser.add_argument("-uc", action="store_true", help="Upload changes listed by -lsu (see settings.ini).")
    parser.add_argument("-tree", action="store_true", help="Upload 'trees' of directories (see settings.ini).")
    parser.add_argument("-log", action="store_true", help="Create a pretty log file of all I/O operations.")
    parser.add_argument("-lsr", nargs='?', const="list", choices=["list", "blacklist", "remove"], help="List synced files that were removed from Google Drive.")
    parser.add_argument("-lsd", action="store_true", help="List changes that will get downloaded (dry run).")
    parser.add_argument("-dc", action="store_true", help="Download changes listed by -lsd.")
    parser.add_argument("-lsffs", nargs=2, help="Fully sync folder_id with local_path (dry run).", metavar=("FOLDER_ID", "LOCAL_PATH"))
    parser.add_argument("-ffs", nargs=2, help="Fully sync folder_id with local_path (-lsffs).", metavar=("FOLDER_ID", "LOCAL_PATH"))
    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        return -1

    with backuper.Backuper(pretty_log=args.log) as b:
        if args.lsu:
            b.list_upload_changes()

        if args.lsd:
            b.download_changes(dry_run=True)

        if args.lsr:
            opt = args.lsr
            if opt == "list":
                b.list_removed_from_gd()
            elif opt == "blacklist":
                b.blacklist_removed_from_gd()
            elif opt == "remove":
                b.remove_db_removed_from_gd()

        if args.lsffs:
            b.full_folder_sync(args.lsffs[0], args.lsffs[1], dry_run=True)
        
        if args.ffs:
            b.full_folder_sync(args.ffs[0], args.ffs[1], dry_run=False)

        if args.uc:
            b.upload_changes()

        if args.dc:
            b.download_changes()

        if args.tree:
            b.upload_tree_logs_zip()

if __name__ == "__main__":
    main()


# NOTE: performance can be improved with caching.

# Sync Google Photos folder in Google Drive (download it)

# 403 doesn't retry!!!!!!!!!!!!!!!

# split Backup class to Backuper, Downloader (or something ...)
# use changes for download sync checking (or multithreaded walk folder?)
# is_for_download -> what if a file changes mid checking? (archived.db)
# unify printing
# improve settings.ini and Config
# settings sync folders (key: value) -> drive_id: save_path 

# uploading based on md5?

# migrate useful sharedtools to pytools
# downloading large files with partial download ?

# android companion app???

# LOGGING.INFO CAN'T HANDLE CHINESE RUNES (UnicodeEncodeError)

# use a general queue for files to upload (increase speed so as not to wait if other folders are done already)

# IF INTERNET IS DEAD -> WAIT!!!

# high ram usage

# ERROR HANDLING
# 403 when uploading

# ADD WILDCARDS TO BLACKLIST (glob.glob)
# better blacklisting / whitelisting


# add better command line progress display
# use tqdm

# ONLY UPDATE DB INFORMATION IF FILE SUCCESSFULLY UPLOADED etc. ....


# DONE:
# migrate to new drive api (v3)
# multithreaded upload
# refactor api
# make log structure and backup at the same time
# incorrect folder id uploading
# batch requests (batch delete files / check for changes ...)
# "The parents field is not directly writable in update requests. Use the addParents and removeParents parameters instead.">

# FIRST UPLOAD FOLDERS ONLY THEN THE FILES
# update files in use (main.py, settings.ini ...)

# UPLOAD FILES WITH ORIGINAL FILE NAME (NOT unify_path())
# FILES FOR SYNC INCORRECT
# download files added to drive to pc
# !!!!!!!!!!!!!!! not all files are being uploaded
