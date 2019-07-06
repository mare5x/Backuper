from backuper import database as db

def list_all():
    with db.GoogleDriveDB() as _:
        for archive in db.DriveArchive.select().iterator():
            print(archive.path)

def list_paths_contains(path):
    with db.GoogleDriveDB() as _:        
        q = db.DriveArchive.select().where(db.DriveArchive.path.contains(path))
        for archive in q.iterator():
            print(archive.path)
    

if __name__ == "__main__":
    list_paths_contains("backuper")