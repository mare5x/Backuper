from backuper import database as db

def list_all():
    for archive in db.DriveArchive.select().iterator():
        print(archive.path)

def list_paths_contains(path):
    q = db.DriveArchive.select().where(db.DriveArchive.path.contains(path))
    for archive in q.iterator():
        print(archive.path)

drive_db = db.GoogleDriveDB()

list_paths_contains("backuper")

drive_db.close()