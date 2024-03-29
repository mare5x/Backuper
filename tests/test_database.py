from backuper import database as db

def list_all():
    with db.GoogleDriveDB() as gddb:
        for archive in gddb:
            print(archive.path)

def list_paths_contains(path):
    with db.GoogleDriveDB() as gddb:        
        q = gddb.model.select().where(gddb.model.path.contains(path))
        for archive in q.iterator():
            print(archive.path)
    

if __name__ == "__main__":
    list_paths_contains("backuper")
    list_all()