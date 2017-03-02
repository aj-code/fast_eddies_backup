import io, sys, argparse, stat, shutil, tempfile, lzma, binascii, json
from datetime import timedelta, timezone

import db
from utils import *
from b2 import B2Threaded


BLOCK_SIZE = 256 * 1024
BLOCK_FORMAT_VERSION = 1
METADATA_STORE_VERSION = 3
METADATA_STORE_FILENAME = 'metadata'
METADATA_STORE_HEADER = b'BACKUP_METADATA'


verbose = False
db_filename = None
cache_dir = None
b2_threaded = None
key_data = None



def backup(dirs):

    restore_db()

    with db.get_db(db_filename) as conn:
        cur = conn.cursor()

        cur.execute(db.SQL_INSERT_FILE_SET)
        backup_set_id = cur.lastrowid

        dir_cache = {}

        for name, is_dir, is_symlink in get_files(dirs):

            if verbose:
                print('Processing', name)

            name = os.path.abspath(name)

            st = os.stat(name, follow_symlinks=False)

            if is_symlink: #handle symlink
                link_dest = os.readlink(name)
                db.handle_symlink(cur, backup_set_id, name, link_dest)
                continue

            if is_dir: #save dir permissions
                set_dir_id = db.handle_dir(cur, name, backup_set_id, st)
                dir_cache[name] = set_dir_id
                continue

            dir_name = os.path.dirname(name)
            if dir_name not in dir_cache:
                raise RuntimeError('BUG: set dir id not found in cache, filesystem walk probably wrong order. Failing.')

            file_name = os.path.basename(name)
            set_dir_id = dir_cache[dir_name]


            prev_file = db.get_prev_file(cur, file_name, dir_name, st)

            #check if new file, or perms have changed do normal backup of file
            #note: in future we could optimise for only perm/owner changes by copying file block map in db instead of scanning file
            if not prev_file:
                if verbose:
                    print('File new or changed, backing up')

                file_id = db.insert_file_for_set(cur, backup_set_id, file_name, set_dir_id, st)
                dedup_and_store(cur, name, file_id)

            else: #exact file already exists

                if verbose:
                    print('File unchanged, updating metadata and continuing')

                file_id = prev_file['id']
                db.map_existing_file_to_set(cur, backup_set_id, file_id)


    save_db()


def dedup_and_store(cur, filepath, file_id):
    with open(filepath, 'rb') as f:

        block_count = 0
        new_count = 0

        file_hasher = hashlib.sha256()
        while True:

            block_data = f.read(BLOCK_SIZE)
            if len(block_data) == 0:
                break

            block_hasher = hashlib.sha256()
            block_hasher.update(block_data)
            block_hash = block_hasher.digest()[16:]  # only take 128 bits

            file_hasher.update(block_data)

            block_id, is_new_block = db.find_or_insert_block(cur, block_hash)

            cur.execute(db.SQL_INSERT_FILE_MAP, (file_id, block_id))

            if is_new_block:
                new_count += 1
                write_block(block_id, block_data)

            block_count += 1

        cur.execute(db.SQL_UPDATE_FILE_HASH, (file_hasher.digest()[16:], file_id))

        if verbose:
            print(filepath)
            if block_count > 0:
                dup_count = (block_count-new_count)
                print('\tDup block count: %d / %d = %f%%' % (dup_count, block_count, ((dup_count / block_count) * 100)))
                print('\tSaved: {0}M'.format((dup_count * BLOCK_SIZE) / (1024 * 1024)))
            else:
                print('\tEmpty file')



def restore(backup_set_id, exclude_globs, include_globs, output_dir):

    if not os.path.exists(output_dir):
        print('Restore dir does not exist, exiting.')
        return

    if os.listdir(output_dir):
        print('Restore dir is not empty, exiting.')
        return

    if not restore_db():
        print("No previous backups exist, exiting.")
        return

    with db.get_db(db_filename) as conn:

        dir_results, symlink_results, file_results = db.get_results_for_set(conn, backup_set_id)

        if not file_results and not symlink_results and not dir_results:
            print('Nothing to restore or incorrect backup set.')
            return

        common_path = os.path.commonpath([f['name'] for f in symlink_results + dir_results]) #don't need to include file as those don't have full paths anyway

        #restore dirs
        for r in dir_results:
            name = r['name']
            gid = r['gid']
            uid = r['uid']
            permissions = r['permissions']

            #skip exludes
            if exclude_globs and matches_any_glob(name, exclude_globs):
                continue

            #skip not included
            if include_globs and not matches_any_glob(name, include_globs):
                continue

            unique_path = name[len(common_path) + 1:]
            if unique_path == '':
                continue

            new_path = os.path.join(output_dir, unique_path)
            print("DIR", new_path)

            os.makedirs(new_path)

            set_file_attributes(new_path, uid, gid, permissions, verbose)

        #restore files
        block_writer = BlockWriter(key_data['crypto_key'], BLOCK_FORMAT_VERSION, verbose=verbose)
        block_writer.start()
        try:
            for r in file_results:
                file_id = r['id']
                name = r['name']
                dir_name = r['dir_name']
                size = r['size']
                file_hash = r['hash']

                permissions = (r['uid'], r['gid'], r['permissions'])

                new_path = os.path.join(output_dir, dir_name[len(common_path)+1:], name)

                # skip excludes
                if exclude_globs and matches_any_glob(new_path, exclude_globs):
                    continue

                # skip not included
                if include_globs and not matches_any_glob(new_path, include_globs):
                    continue

                if not os.path.exists(dir_name): #dir may not exist due to include/exclude checks on dir paths
                    os.makedirs(dir_name)

                if os.path.exists(new_path):
                    print('File already exists in output directory. Bailing before data overwrite.')
                    return

                block_results = conn.execute(db.SQL_SELECT_FILE_BLOCKS, (file_id,)).fetchall()
                total_blocks = len(block_results)

                if verbose:
                    print('Download beginning for', name, 'need', total_blocks, 'blocks')

                #create file and allocate space
                fd = open(new_path, 'wb')
                if size > 0:
                    fd.seek(size-1)
                    fd.write(b'\0')
                    fd.seek(0)

                file_offset = 0
                for br in block_results: #block_results must be sorted
                    block_id = br['block_id']

                    future = b2_threaded.download(block_id, stream=False)
                    block_writer.put(fd, future, file_offset, total_blocks, file_hash, permissions)
                    file_offset += BLOCK_SIZE


        finally:
            block_writer.shutdown()


        #restore symlinks
        for r in symlink_results:
            name = r['name']
            dest = r['dest']

            #skip exludes
            if exclude_globs and matches_any_glob(name, exclude_globs):
                continue

            #skip not included
            if include_globs and not matches_any_glob(name, include_globs):
                continue

            new_path = os.path.join(output_dir, name[len(common_path) + 1:])
            print('SYMLINK', new_path)

            if os.path.exists(new_path):
                print('File already exists in output directory. Bailing before data overwrite.')
                return

            os.symlink(dest, new_path)

            #note, perms not updated, because symlink perms are from the dest file


def auto_delete_sets():

    if not restore_db():
        print("No previous backups exist, exiting.")
        return

    with db.get_db(db_filename) as conn:
        sets = conn.execute(db.SQL_SELECT_SETS).fetchall()

    now = datetime.now()

    sets_to_delete = []

    # handle between 1 and 4 weeks, keep one per week
    for weeks_ago in range(2, 5):
        start_time = now - timedelta(days=weeks_ago * 7)
        week_sets = get_sets_within_time(sets, start_time, timedelta(days=7))
        if len(week_sets) > 1:
            # delete newest
            for s in week_sets[:-1]:
                sets_to_delete.append(s['id'])

    # handle 1 to 5 months ago, keep one per month
    for months_ago in range(2, 6):
        start_time = now - timedelta(days=months_ago * 30)
        month_sets = get_sets_within_time(sets, start_time, timedelta(days=30))
        if len(month_sets) > 1:
            # delete newest
            for s in month_sets[:-1]:
                sets_to_delete.append(s['id'])


    # handle more than 6 months, delete all
    for s in sets:
        six_months = timedelta(days=30 * 6)
        if sqlite_date_to_datetime(s['created']) < now - six_months:
            sets_to_delete.append(s['id'])


    if len(sets_to_delete) > 0: #defer actuall delete to here so we don't unnecessarily query all file in b2
        known_b2_files = get_files_in_b2(b2_threaded)
        for set_id in sets_to_delete:
            _delete_set(conn, known_b2_files, set_id)

        db.optimise_db(db_filename)
        save_db()

    elif verbose:
        print('No backup sets found for deletion')



def delete_set(backup_set_id):
    restore_db()

    known_b2_files = get_files_in_b2(b2_threaded)

    with db.get_db(db_filename) as conn:
        _delete_set(conn, known_b2_files, backup_set_id)

    db.optimise_db(db_filename)
    save_db()


def _delete_set(conn, known_b2_files, backup_set_id):

    if verbose:
        print('Deleting backup set: ', backup_set_id)

    cur = conn.cursor()

    cur.execute(db.SQL_DELETE_SET, (backup_set_id,))
    if cur.rowcount == 0:
        print("Backup set doesn't exist. Failing.")
        return

    if not known_b2_files:
        future = b2_threaded.list_files()
        future.lock.acquire()

        known_b2_files = {}
        for b2_file in future.response:
            known_b2_files.setdefault(b2_file.name, []).append(b2_file)

    dir_results, symlink_results, file_results = db.get_results_for_set(cur, backup_set_id)

    #del files
    for f in file_results:
        if verbose:
            full_name = os.path.join(f['dir_name'], f['name'])
            print('Deleting: set(%d) %s' % (backup_set_id, full_name))

        result = cur.execute(db.SQL_SELECT_OTHER_SET_FILE_MAP_COUNT, (backup_set_id, f['id'])).fetchone()
        if result['count'] == 0: #file not used in other set
            delete_file(cur, f['id'], known_b2_files)

        #delete mapping
        cur.execute(db.SQL_DELETE_SET_FILE_MAP, (backup_set_id, f['id']))


    #del symlinks
    for s in symlink_results:
        if verbose:
            print('Deleting: set(%d) %s' % (backup_set_id, s['name']))

        result = cur.execute(db.SQL_SELECT_OTHER_SET_SYMLINK_MAP_COUNT, (backup_set_id, s['id'])).fetchone()
        if result['count'] == 0: #file not used in other set
            cur.execute(db.SQL_DELETE_SYMLINK, (s['id'],))

        #delete mapping
        cur.execute(db.SQL_DELETE_SET_SYMLINK_MAP, (backup_set_id, s['id']))

    #del dirs
    for d in dir_results:
        if verbose:
            print('Deleting: set(%d) %s' % (backup_set_id, d['name']))

        result = cur.execute(db.SQL_SELECT_OTHER_SET_DIR_MAP_COUNT, (backup_set_id, d['id'])).fetchone()
        if result['count'] == 0: #file not used in other set
            cur.execute(db.SQL_DELETE_DIR, (d['id'],))

        #delete mapping
        cur.execute(db.SQL_DELETE_SET_DIR_MAP, (backup_set_id, d['id']))


    cur.close()
    conn.commit()



def verify_and_clean(delete_unrecoverable):

    if not restore_db():
        print("No previous backups exist, exiting.")
        return


    with db.get_db(db_filename) as conn:

        if verbose:
            print('Downloading B2 file list')

        future = b2_threaded.list_files()
        future.lock.acquire()
        b2_files = future.response

        if verbose:
            print('Checking for orphaned blocks and old files in B2')

        known_b2_files = {}
        for f in b2_files:

            #check of multiple uploads, delete oldest. Will happen on failed backup, and normally with metadata upload
            known_b2_files.setdefault(f.name, []).append(f)
            if len(known_b2_files[f.name]) > 1:
                if verbose:
                    print('Old unused data discovered for deletion:', f.name)
                of = known_b2_files[f.name]
                if of[0].upload_timestamp > of[1].upload_timestamp:
                    b2_threaded.delete(of[1].name, of[1].b2_id)
                    del of[1]
                else:
                    b2_threaded.delete(of[0].name, of[0].b2_id)
                    del of[0]

            #check block is in the db, or metadata
            if f.name != METADATA_STORE_FILENAME:
                result = conn.execute(db.SQL_SELECT_BLOCK_BY_ID, (f.name,)).fetchone()
                if not result:
                    if verbose:
                        print('Old unused data discovered for deletion:', f.name)

                    b2_threaded.delete(f.name, f.b2_id)
                    del known_b2_files[f.name]

        if verbose:
            print('Verifying expected blocks exist in B2')

        #ensure we have something in b2 for each block
        missing = []
        for b in conn.execute(db.SQL_SELECT_BLOCKS).fetchall():
            if str(b['id']) not in known_b2_files:
                missing.append(b['id'])

        is_db_modified = False
        if missing:
            print()
            print("Verification Error: one or more blocks is missing from b2 storage. Some backup's may be corrupt.")
            print("This is most likely caused by an incomplete set deletion, in which case just delete the set(s) and "
                  "verify again.")
            print()
            print("Otherwise, an immediate backup into a NEW bucket is recommended to ensure at least one backup is available.")
            print("Future backups based on this set will be corrupt unless fixed with the RISKY --delete-unrecoverable "
                  "switch, which you should only do if you really know what you're doing.")
            print()
            print("The following files are affected:")
            for b_id in missing:
                for f in conn.execute(db.SQL_SELECT_FILE_BY_BLOCK, (b_id,)).fetchall():
                    print('Set(%d): %s' % (f['backup_set_id'], f['name']))
                    if delete_unrecoverable:
                        print('\tdeleting file.')
                        delete_file(conn, f['id'], known_b2_files)
                        is_db_modified = True
            print()


    if is_db_modified:
        save_db()

    if verbose:
        print('Complete.')


def delete_file(conn, file_id, known_b2_files):

    conn.execute(db.SQL_DELETE_FILE, (file_id,))

    map_to_delete = []
    block_to_delete = set() #set because same file may reference same block mulitple times and we only need to delete once

    results = conn.execute(db.SQL_SELECT_BLOCK_AND_COUNT_BY_FILE, (file_id,file_id)).fetchall()
    count = 0
    total = len(results)
    for r in results:
        count +=1

        map_to_delete.append(r['id'])

        if r['other_count'] == 0: #only this file using it
            block_to_delete.add(r['block_id'])

            key = str(r['block_id'])
            if key in known_b2_files: #might not exist in B2
                for f in known_b2_files[key]: #may be multiple versions
                    b2_threaded.delete(key, f.b2_id)
                del known_b2_files[key]

        #delete from db in chunks, or when loop is about done
        if len(map_to_delete) >= 100 or (count == total and map_to_delete):
            sql = db.SQL_DELETE_FILE_BLOCK_MAPS % ', '.join('?' * len(map_to_delete))
            r = conn.execute(sql, map_to_delete)
            if verbose:
                print('\tDeleted %d map entries' % r.rowcount)

            map_to_delete = []

        if len(block_to_delete) >= 100 or (count == total and block_to_delete):
            sql = db.SQL_DELETE_BLOCKS % ', '.join('?' * len(block_to_delete))
            r = conn.execute(sql, list(block_to_delete))
            if verbose:
                print('\tDeleted %d block entries' % r.rowcount)

            block_to_delete = set()


def write_block(block_id, block_data):

    compressed = gzip.compress(block_data, compresslevel=6)
    encrypted = encrypt_block(key_data['crypto_key'], BLOCK_FORMAT_VERSION, compressed)

    b2_threaded.upload(str(block_id), encrypted)


#TODO, delete previous metadata entry in b2
def save_db():

    cache_filename = os.path.join(cache_dir, METADATA_STORE_FILENAME)
    with open(cache_filename, 'wb') as enc_db:

        iv = os.urandom(16)
        cipher = Cipher(algorithms.AES(key_data['crypto_key']), modes.GCM(iv), backend=default_backend())
        encryptor = cipher.encryptor()

        enc_db.write(METADATA_STORE_HEADER)
        enc_db.write(struct.pack('B', METADATA_STORE_VERSION))
        enc_db.write(iv)

        lzc = lzma.LZMACompressor()

        with open(db_filename, 'rb') as db_file:
            while True:
                buf = db_file.read(16 * 1024)
                if len(buf) == 0:
                    break
                buf_comp = lzc.compress(buf)
                buf_enc = encryptor.update(buf_comp)
                enc_db.write(buf_enc)

        buf_comp = lzc.flush()
        enc_db.write(encryptor.update(buf_comp) + encryptor.finalize())

        if len(encryptor.tag) != 16:
            raise RuntimeError('BUG: GCM auth tag unexpected size. Failing.')

        enc_db.write(encryptor.tag)

    if verbose:
        print('Metatdata size:', os.stat(enc_db.name).st_size)

    with open(cache_filename, 'rb') as enc_db:
        b2_threaded.wait_on_all_complete() #ensure all blocks are uploaded before metadata, better crash protection
        b2_threaded.upload(METADATA_STORE_FILENAME, enc_db)
        b2_threaded.wait_on_all_complete()


def restore_db():

    use_cache = False
    cache_filename = os.path.join(cache_dir, METADATA_STORE_FILENAME)
    if os.path.exists(cache_filename):

        #get sha1 of metadata in b2, check against local cache
        future = b2_threaded.get_file_data(METADATA_STORE_FILENAME)
        future.lock.acquire()

        if future.response:
            if future.response.sha1 == sha1_file(cache_filename):
                use_cache = True
                if verbose:
                    print("Restoring metadata from local cache")


    if not use_cache:

        if verbose:
            print("Restoring metadata from B2")

        future = b2_threaded.download(METADATA_STORE_FILENAME, stream=True)
        future.lock.acquire()

        download_stream = future.response
        if download_stream is None:
            if verbose:
                print("No metadata found to restore, no previous backups exist.")
                print()
            return False

        with open(cache_filename, 'wb') as f:
            try:
                shutil.copyfileobj(download_stream, f) #do actual download
            finally:
                download_stream.close()


    metadata_version = None
    try:
        with open(cache_filename, 'rb') as enc_db:

            enc_db.seek(-16, io.SEEK_END)
            auth_tag = enc_db.read(16) #read GCM auth tag
            enc_db.seek(0)

            if enc_db.read(len(METADATA_STORE_HEADER)) != METADATA_STORE_HEADER:
                raise RuntimeError('Metadata file missing header, possibly corrupt.')

            metadata_version = struct.unpack('B', enc_db.read(1))[0]

            iv = enc_db.read(16)

            lzd = lzma.LZMADecompressor()
            cipher = Cipher(algorithms.AES(key_data['crypto_key']), modes.GCM(iv, tag=auth_tag), backend=default_backend())
            decryptor = cipher.decryptor()

            ciphertext_size = os.stat(cache_filename).st_size - enc_db.tell() - 16 #minus already read and auth tag
            with open(db_filename, 'wb') as db_file:
                for buf_enc in read_range(enc_db, enc_db.tell(), ciphertext_size):
                    buf_comp = decryptor.update(buf_enc)
                    buf = lzd.decompress(buf_comp)
                    db_file.write(buf)

                db_file.write(decryptor.finalize())

    except lzma.LZMAError as e:
        print('Metadata unreadable, likely incorrect encryption key is used. Failing.')
        sys.exit(1)

    #migrate from old metadata formats
    if metadata_version != METADATA_STORE_VERSION:
        raise RuntimeError('Incorrect metadata version and upgrade not supported. You may need to empty your B2 bucket and start a fresh backup.')

    return True


def gen_keyfile(key_file):
    if os.path.isfile(key_file):
        print("Keyfile already exists, exiting.")
        sys.exit(1)


    file_data = {}

    file_data['b2_account_id'] = input("Enter your B2 account ID: ")
    file_data['b2_application_key'] = input("Enter your B2 application key: ")
    file_data['b2_bucket'] = input("Enter your B2 bucket name (this must be an empty bucket): ")

    file_data['crypto_key'] = binascii.hexlify(os.urandom(32)).decode('ASCII')


    with open(key_file, 'w') as f:
        f.write(json.dumps(file_data, indent=4))

    os.chmod(key_file, stat.S_IRUSR | stat.S_IRGRP | stat.S_IWUSR)

    print("Key file generated, store this carefully (preferably offline as well) otherwise you wont be able to restore data.")


def load_keyfile(key_file):

    if not os.path.exists(key_file):
        raise RuntimeError("Keyfile %s doesn't exist" % key_file)

    with open(key_file, 'r') as f:
        key_data = json.loads(f.read().strip())

    key_data['crypto_key'] = binascii.unhexlify(key_data['crypto_key'])

    if len(key_data['crypto_key']) != 32:
        raise RuntimeError('BUG: block encryption key incorrect size')

    return key_data


def list_sets():

    if not restore_db():
        print("No previous backups exist, exiting.")
        return

    with db.get_db(db_filename) as conn:

        print('ID\t\tCreated')
        print('========================')

        results = conn.execute(db.SQL_SELECT_SETS).fetchall()
        for r in results:
            #convert from UTC
            created = sqlite_date_to_datetime(r['created']).replace(tzinfo=timezone.utc).astimezone(tz=None)
            created_str = created.strftime('%Y-%m-%d %H:%M:%S')

            print('%d\t\t%s' % (r['id'], created_str))

        print()


def list_contents(backup_set_id):

    if not restore_db():
        print("No previous backups exist, exiting.")
        return

    with db.get_db(db_filename) as conn:

        dir_results, symlink_results, file_results = db.get_results_for_set(conn, backup_set_id)

        names = []
        for r in file_results:
            names.append(os.path.join(r['dir_name'], r['name']))

        for r in symlink_results:
            names.append(r['name'])

        for r in dir_results:
            names.append(r['name'])

        if len(names) == 0:
            print('Backup set incorrect or empty.')

        names.sort()
        for n in names:
            print(n)


def download_metadata(dest_path):
    if not restore_db():
        print("No previous backups exist, exiting.")
        return

    shutil.copy(db_filename, dest_path)

    print("Metadata SQLite database stored at:", dest_path)


def main():

    os.umask(int('077', 8))  # restrict created files to this user

    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--key-file', help='Encryption keyfile, generate this with the genkey mode.',
                        required=True)
    parser.add_argument('--mode', help='Mode to run.',
                        choices=['backup', 'restore', 'genkey', 'listsets', 'listcontents', 'deleteset', 'autodeletesets', 'verifyandclean', 'downloadmetadata'], required=True)
    parser.add_argument('--include', metavar='directory', action='append',
                        help='One or more directories to backup.', nargs="+")
    parser.add_argument('--exclude', metavar='path glob', action='append',
                        help='Path glob to exclude from restore.', nargs="+")
    parser.add_argument('-s', '--backup-set', type=int,
                        help='Which backup set to restore or delete, find this through the listsets mode.')
    parser.add_argument('--output-dir', help='Location to restore files to.')
    parser.add_argument('-v', '--verbose', help="increase output verbosity", action="store_true")
    parser.add_argument('-d', '--cache-dir', help="directory for temporary files", required=True)
    parser.add_argument('-t', '--threads', help="Threads for upload/download.", type=int, default=10)
    parser.add_argument('--delete-unrecoverable', help="Delete file entries during verifyandclean where blocks are found "
                                                       "to be missing. This allows a corrupt fileset to be partially recovered "
                                                       "while sacrificing data associated with unrecoverable files. If "
                                                       "unrecoverable files are important you may wish to attempt a unsupported manual "
                                                       "restore before sacrificing the available data.",
                                                    action="store_true")
    args = parser.parse_args()

    global verbose
    verbose = args.verbose


    if args.mode == 'genkey': #the rest can't happen until we have a key file
        gen_keyfile(args.key_file)
        return

    global key_data
    key_data = load_keyfile(args.key_file)

    global cache_dir
    cache_dir = args.cache_dir
    if not os.path.exists(cache_dir):
        print("Cache directory doesn't exist. Exiting.")
        return

    global db_filename
    db_filename = tempfile.mktemp(dir=cache_dir)


    global b2_threaded
    b2_threaded = B2Threaded(key_data['b2_account_id'], key_data['b2_application_key'], key_data['b2_bucket'],
                             threads=args.threads, verbose=verbose)


    if args.mode == 'listsets':
        list_sets()

    if args.mode == 'autodeletesets':
        auto_delete_sets()

    if args.mode == 'deleteset':
        if not args.backup_set:
            print("--backup-set missing")
        else:
            delete_set(args.backup_set)

    if args.mode == 'listcontents':
        if not args.backup_set:
            print("--backup-set missing")
        else:
            list_contents(args.backup_set)

    if args.mode == 'verifyandclean':
        verify_and_clean(args.delete_unrecoverable)

    if args.mode == 'downloadmetadata':
        download_metadata('backup_metadata.db')

    if args.mode == 'backup':
        if not args.include:
            print("--include missing")
        elif args.exclude:
            print('--exclude only supported during restore')
        else:
            include_dirs = [item for sublist in args.include for item in sublist]
            backup(include_dirs)

    if args.mode == 'restore':
        if not args.output_dir:
            print("--output-dir missing")
        elif not args.backup_set:
            print("--backup-set missing")
        else:
            exclude_globs = []
            if args.exclude:
                exclude_globs = [item for sublist in args.exclude for item in sublist] #gather multiple switches

            include_globs = []
            if args.include:
                include_globs = [item for sublist in args.include for item in sublist]

            restore(args.backup_set, exclude_globs, include_globs, args.output_dir)


if __name__ == "__main__":
    try:
        main()
    finally:
        if b2_threaded is not None:
            b2_threaded.shutdown()
        if db_filename is not None and os.path.exists(db_filename):
            os.unlink(db_filename)