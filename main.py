import backuper
import concurrent.futures
import argparse
import os.path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--googledrive", action="store_false",
                        default=True, help="DISABLE sync with Google Drive")
    parser.add_argument("-d", "--dropbox", action="store_true",
                        default=False, help="sync with Dropbox")
    parser.add_argument("-b", "--backupsync", action="store_false",
                        default=True, help="DON'T backup sync paths")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", "--downloadsync", action="store_true",
                        default=False, help="download sync paths")
    group.add_argument("-c", "--downloadchanges", action="store_false",
                        default=True, help="DON'T download changes made")
    parser.add_argument("--blacklist", action="store_false",
                        default=True, help="DON'T blacklist files removed from cloud storage")
    parser.add_argument("--deletedeleted", action="store_true",
                        default=False, help="delete files removed from disk from cloud storage")
    parser.add_argument("--logstructures", action="store_false",
                        default=True, help="DON'T make a structure log of paths")
    parser.add_argument("-l", "--log", action="store_false",
                        default=True, help="DON'T make a log file")
    parser.add_argument("-w", "--overwrite", action="store_true",
                        default=False, help="overwrite existing files when downloading")

    args = parser.parse_args()

    with backuper.Backup(google=args.googledrive, my_dropbox=args.dropbox, log=args.log) as bkup:
        paths = bkup.read_paths_to_backup()

        if args.blacklist:
            bkup.blacklist_removed_from_gdrive(log=True)

        if args.deletedeleted:
            bkup.del_removed_from_local(progress=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            if args.logstructures:
                executor.submit(backuper.upload_log_structures, bkup)

            if args.backupsync or args.downloadsync:
                executor.submit(backuper.google_drive_sync, bkup, args.backupsync, args.downloadsync, paths['dirs_to_archive'])
            
            if args.downloadchanges:
                executor.submit(bkup.download_sync_changes, args.overwrite)

        print("\nDONE")


    # backuper.backup(dropbox=args.dropbox, google_drive=args.googledrive, backup_sync=args.backupsync, 
    #                 blacklist=args.blacklist, delete_deleted=args.deletedeleted, 
    #                 log_structures=args.logstructures, log=args.log, download_sync=args.downloadsync,
    #                 download_changes=args.downloadchanges, overwrite=args.overwrite)


if __name__ == "__main__":
    main()


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

# download files added to drive to pc

# add better command line progress display
# use tqdm

# ONLY UPDATE DB INFORMATION IF FILE SUCCESSFULLY UPLOADED etc. ....

# !!!!!!!!!!!!!!! not all files are being uploaded

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
