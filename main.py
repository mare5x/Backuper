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

    class _FullFolderSyncAction(argparse.Action):
        help_str = "Fully sync folder_id with local_path. Usage: -ffs FOLDER_ID LOCAL_PATH [dry/sync]"
        options = ("dry", "sync")
        
        def __init__(self, *args, nargs=None, metavar=None, **kwargs):
            super().__init__(*args, nargs='*', help=self.help_str, metavar=("folder_id local_path", "dry"), **kwargs)
        
        def __call__(self, parser, namespace, values, option_string):
            n = len(values)
            if n < 2 or n > 3: return parser.error("Invalid number of arguments for -ffs!")
            # Dry run if 'dry' or left out. If 'sync' sync, otherwise raise error.
            if n == 2:
                values.append(options[0])
            elif n == 3:
                if values[2] not in self.options:
                    return parser.error("Unknown argument for -ffs!")
            setattr(namespace, self.dest, values)

    class _MirrorAction(argparse.Action):
        help_str = "Mirror all sync_dirs (settings.ini) onto Google Drive. Usage: -mir [fast/full] [dry/sync] (default: fast dry)"
        options1 = ("fast", "full")
        options2 = ("dry", "sync")

        def __init__(self, *args, nargs=None, metavar=None, **kwargs):
            super().__init__(*args, nargs='*', help=self.help_str, metavar=("fast/full", "dry/sync"), **kwargs)
        
        def __call__(self, parser, namespace, values, option_string):
            options1, options2 = self.options1, self.options2
            n = len(values)
            if n == 0:
                values.append(options1[0])
                values.append(options2[0])
            else:
                if values[0] not in options1:
                    return parser.error("Invalid option 1 for -mir!")
                if n == 1:
                    values.append(options2[0])
                if n == 2:
                    if values[1] not in options2:
                        return parser.error("Invalid option 2 for -mir!")
                else:
                    return parser.error("Too many arguments for -mir!")
            setattr(namespace, self.dest, values)

    parser.add_argument("-uc", nargs='?', const="list", choices=["list", "upload"], help="Upload changes listed by 'list' (see settings.ini).")
    parser.add_argument("-dc", nargs='?', const="list", choices=["list", "download"],  help="Download changes listed by 'list'.")
    parser.add_argument("-tree", action="store_true", help="Upload 'trees' of directories (see settings.ini).")
    parser.add_argument("-rem", nargs='?', const="list", choices=["list", "blacklist", "remove"], help="List synced files that were removed from Google Drive.")
    parser.add_argument("-ffs", action=_FullFolderSyncAction)
    parser.add_argument("-mir", action=_MirrorAction)
    parser.add_argument("-nolog", action="store_false", help="DON'T create a pretty log file of all I/O operations.")
    args = parser.parse_args()

    # Check if any option is actually set.
    exclude = ["nolog"]
    if not any(getattr(args, key) for key in filter(lambda key: key not in exclude, vars(args))):
        parser.print_help()
        return -1

    with backuper.Backuper(pretty_log=args.nolog) as b:
        if args.rem:
            opt = args.rem
            if opt == "list":
                b.list_removed_from_gd()
            elif opt == "blacklist":
                b.blacklist_removed_from_gd()
            elif opt == "remove":
                b.remove_db_removed_from_gd()

        if args.mir:
            _type, dry_run = args.mir
            b.mirror_all(fast=(_type == "fast"), dry_run=(dry_run == "dry"))
        
        if args.ffs:
            folder_id, local_path, dry_run = args.ffs
            b.full_folder_sync(folder_id, local_path, dry_run=(dry_run == "dry"))

        if args.uc:
            opt = args.uc
            if opt == "list": 
                b.list_upload_changes()
            elif opt == "upload": 
                b.upload_changes()

        if args.dc:
            b.download_changes(dry_run=(args.dc == "list"))

        if args.tree:
            b.upload_tree_logs_zip()

if __name__ == "__main__":
    main()
