# Fast Eddie's Backup
A opensource backup solution for Linux featuring deduplication, compression,
local encryption, and backed by Backblaze B2.

Fast Eddie's lets you keep take multiple backups (each backup is called a backup set) while only storing changed data. Backups are 
incremental by splitting files into chunks and only uploading chunks that haven't been seen before 
in any file within any backup set. The software is designed for use on relatively modern linux using at least python 3.5
and to be run regularly via cron (or similar).

All data uploaded to B2 is encrypted before upload using authenticated AES256-GCM.


## Install
Make sure you've got python 3.5+ and git installed, and an active B2 account.

**Grab the code**

`wget https://github.com/aj-code/fast_eddies_backup/archive/master.zip && unzip master.zip`

or

`git clone https://github.com/aj-code/fast_eddies_backup.git`



**Install the dependencies**

`apt install python3-cryptography python3-requests`

or

`pip install cryptography requests`



**Create a cache directory (anywhere you like)**

`mkdir backup_cache`


**Generate your keyfile**

`python3 run.py --mode genkey -k backup_secret.key -d backup_cache`


## Quick Usage

Run a backup:

`python3 run.py --mode backup --key-file backup_secret.key --cache-dir backup_cache --include /full/path/to/stuff --include /full/path/to/other/stuff`


List the current backup sets:

`python3 run.py --mode listsets --key-file backup_secret.key --cache-dir backup_cache`



Restore a backup set:

`python3 run.py --mode restore --key-file backup_secret.key --cache-dir backup_cache --output-dir /some/restore_dir/ --backup-set 99`


View the usage help: 

`python run.py --help`

**Modes**
 - backup, restore, listsets, deleteset : fairly self explanatory
 - verifyandclean : checks that the expected file chunks exist and removes any orphaned chunks and old metadata backups
 - listcontents : lists the contents of a backup set
 - autodeletesets : when run regularly this keeps all backup sets within the last week, one per week within the last month,
    and one per month within the last 6 months. All other sets are deleted.

## Tips

Make sure you have a separate bucket for each install of Fast Eddie's, your backups will probably be corrupted otherwise.

Carefully copy and store your keyfile, it's the only way to restore from a backup!

Regularly check for new backup sets to ensure backups are working if you're running Fast Eddie's automatically.
Semi-regularly run the verifyandclean mode and check the output for errors.
On occasion do full restore tests to ensure everything is really working.
No backup solution is bulletproof, this one is no exception and (being opensource) comes with no warranty.


