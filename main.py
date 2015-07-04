from files import structurebackup
import tqdm


def backup(dropbox=False, google_drive=False, clean=True, delete_deleted=False):
    if dropbox:
        my_dropbox = structurebackup.Dropbox(overwrite=True)
    else:
        my_dropbox = None
    if google_drive:
        my_google = structurebackup.GoogleDrive()
    else:
        my_google = None

    with structurebackup.Backup(my_google=my_google, my_dropbox=my_dropbox) as bkup:
        paths = bkup.read_paths_to_backup()

        bkup.del_removed_from_drive(log=True)
        if delete_deleted:
            bkup.del_removed_from_local(log=True)

        with bkup.temp_dir(clean=clean) as temp_dir_path:
            for path in tqdm.tqdm(paths['paths_to_backup']):
                bkup.write_log_structure(save_to=temp_dir_path, path=path)

            for path in tqdm.tqdm(paths['dir_only_paths']):
                bkup.write_log_structure(save_to=temp_dir_path, path=path, dirs_only=True)

        for path in tqdm.tqdm(paths['dirs_to_archive']):
            bkup.to_google_drive(path)

        print("\nDONE")

def main():
    backup(google_drive=True, delete_deleted=True)


if __name__ == "__main__":
    main()
