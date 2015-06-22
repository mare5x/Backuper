from files import structurebackup
import tqdm

# paths_to_backup = [r"c:\users\mare5\documents",
#                    r"c:\users\mare5\projects",
#                    r"c:\users\mare5\pictures",
#                    r"d:\guitar",
#                    r"c:\users\mare5\music",
#                    r"d:\ebooks",
#                    r"d:\art"]

# dir_only_paths = [r"d:\downloads",
#                   r"c:\program files",
#                   r"c:\program files (x86)\osu!"]

# dirs_to_archive = [r"c:\users\mare5\projects",
#                    r"d:\ebooks\calibre",
#                    r"c:\users\mare5\documents\šola",
#                    r"d:\guitar\progress",
#                    r"C:\Program Files (x86)\osu!\Screenshots",
#                    r"C:\Program Files (x86)\osu!\Replays"]


def backup(dropbox=True, google_drive=True, clean=True):
    if dropbox:
        my_dropbox = structurebackup.Dropbox(overwrite=True)
    else:
        my_dropbox = None
    if google_drive:
        my_google = structurebackup.GoogleDrive()
    else:
        my_google = None

    with structurebackup.Backup(clean=clean, my_google=my_google, my_dropbox=my_dropbox) as bkup:
        paths = bkup.get_paths_to_backup()
        # for path in tqdm.tqdm(paths['paths_to_backup']):
        #     bkup.write_backup_file(save_to=bkup.temp_dir_path, path=path)

        for path in tqdm.tqdm(paths['dir_only_paths']):
            bkup.write_backup_file(save_to=bkup.temp_dir_path, path=path, get_dirs_only=True)

        # for path in tqdm.tqdm(paths['dirs_to_archive']):
        #     bkup.to_google_drive(path)
        # for path in tqdm.tqdm(dirs_to_archive):
        #     structurebackup.zip_dir(path, name=structurebackup.name_from_path(path), save_path=bkup.temp_dir_path + "\\")


def main():
    backup(False, False, False)


if __name__ == "__main__":
    main()
