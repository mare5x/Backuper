from files import structurebackup
import tqdm
import concurrent.futures


def backup(dropbox=False, google_drive=False, clean=True, delete_deleted=False, log=False):
    if dropbox:
        my_dropbox = structurebackup.Dropbox(overwrite=True)
    else:
        my_dropbox = None
    if google_drive:
        my_google = structurebackup.GoogleDrive()
    else:
        my_google = None

    with structurebackup.Backup(my_google=my_google, my_dropbox=my_dropbox, log=log) as bkup:
        paths = bkup.read_paths_to_backup()

        bkup.del_removed_from_drive(log=True)
        if delete_deleted:
            bkup.del_removed_from_local(log=True)

        with bkup.temp_dir(clean=clean) as temp_dir_path:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                for path in tqdm.tqdm(paths['paths_to_backup']):
                    executor.submit(bkup.write_log_structure, save_to=temp_dir_path, path=path)

                for path in tqdm.tqdm(paths['dir_only_paths']):
                    executor.submit(bkup.write_log_structure, save_to=temp_dir_path, path=path, dirs_only=True)

        for path in tqdm.tqdm(paths['dirs_to_archive']):
            bkup.to_google_drive(path)

        print("\nDONE")

def main():
    backup(google_drive=True, delete_deleted=True, log=True)


if __name__ == "__main__":
    main()
