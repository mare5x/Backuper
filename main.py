from backuper import backuper
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-lsu", action="store_true", help="List changes that will get uploaded.")
    parser.add_argument("-uc", action="store_true", help="Upload changes listed by -lsu (see settings.ini).")
    parser.add_argument("-tree", action="store_true", help="Upload 'trees' of directories (see settings.ini).")
    parser.add_argument("-log", action="store_true", help="Create a pretty log file of all I/O operations.")
    args = parser.parse_args()
    
    with backuper.Backuper(pretty_log=args.log) as b:
        if args.lsu:
            b.list_upload_changes()

        if args.uc:
            b.upload_changes()

        if args.tree:
            b.upload_tree_logs_zip()

if __name__ == "__main__":
    main()


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
