# Backuper

Command-line tool for syncing with Google Drive.

## About

_Backuper_ manages uploading and downloading user defined directories to and from Google Drive. It can also make 'tree' logs of specified directories and upload them.   
  
Settings are customizable (see the generated _settings.ini_ file).

_Backuper_ uses the official Google Drive API for Python.

### History

First written in 2015. Refactored in 2019. 

## Usage

```
usage: backuper.py [-h] [-uc [{list,upload}]] [-dc [{list,download}]] [-tree]
                   [-rem [{list,blacklist,remove}]]
                   [-ffs [folder_id local_path [dry ...]]]
                   [-mir [fast/full [dry/sync ...]]] [-nolog] [-init]

optional arguments:
  -h, --help            show this help message and exit
  -uc [{list,upload}]   Upload changes listed by 'list' (see settings.ini).
  -dc [{list,download}]
                        Download changes listed by 'list'.
  -tree                 Upload 'trees' of directories (see settings.ini).
  -rem [{list,blacklist,remove}]
                        List synced files that were removed from Google Drive.
  -ffs [folder_id local_path [dry ...]]
                        Fully sync folder_id with local_path. Usage: -ffs
                        FOLDER_ID LOCAL_PATH [dry/sync]
  -mir [fast/full [dry/sync ...]]
                        Mirror all sync_dirs (settings.ini) onto Google Drive.
                        Usage: -mir [fast/full] [dry/sync] (default: fast dry)
  -nolog                DON'T create a pretty log file of all I/O operations.
  -init                 Initialize program for first time use.
```

### Extra 

For Backuper to work properly, all synced folders should be inside the generated _/My Drive/Backuper/_ folder.

To find the folder_id of a folder on Google Drive open the folder using your web browser. You can then find the id from the url: https://drive.google.com/drive/folders/THIS-IS-YOUR-FOLDER-ID.

Steps to sync a _specific_ remote folder with local path.
  1. ```py backuper.py -ffs [folder id] [local path] sync```
  2. Add _local path_ to _sync\_dirs_ in _settings.ini_.
  3. For future syncing use normal sync commands (not -ffs).

To remove a synced folder on Google Drive and stop tracking it:
  1. Remove the folder online.
  2. ```py backuper.py -rem remove```
  3. Remove it from _settings.ini_.

When downloading changes from Google Drive, if there is a conflict, you will be prompted on how to resolve the conflict.

## Setup

  1. Clone this repo.
  2. Install [required packages](#requirements).
  3. Follow [First time setup](#first-time-setup).

To run tests use the module format: E.g. ```py -m tests.test_uploader```.

### First time setup

When running the program for the first time, you will have to authorize Backuper to gain access to your Google Drive storage. But before that, you must follow these steps to obtain the required 'client_secret.json' file:
  1. Go to [Google APIs Console](https://console.developers.google.com/) and make a new project.
  2. Enable the Google Drive API.
  3. Click 'Create credentials' and select ‘OAuth client ID’.
  4. Follow instructions to complete the setup and then find and click 'Download JSON'.
  5. Rename the downloaded file to 'client_secret.json' and move it to next to 'backuper.py'.

Before syncing you will also have to fill out the generated _settings.ini_ file. This file will be created automatically the first time you run ```py backuper.py -init```.

### Requirements

_Backuper_ is a Python3 program. You will need the following packages installed: 
  * [google-api-python-client](https://github.com/googleapis/google-api-python-client), [oauth2client](https://github.com/googleapis/oauth2client)
  * [peewee](https://github.com/coleifer/peewee)
  * [pytools](https://github.com/mare5x/pytools)


