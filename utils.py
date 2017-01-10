import sqlite3, os, struct, threading, gzip, queue, hashlib, fnmatch
from datetime import datetime
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


def set_file_attributes(path, uid, gid, permissions, verbose):
    try:
        os.chown(path, uid, gid)
    except PermissionError:
        if verbose:
            print("Warning, permission error setting uid %d on file/dir" % uid)

    try:
        os.chmod(path, permissions)
    except PermissionError:
        if verbose:
            print("Warning, permission error setting gid %d on file/dir" % gid)


def matches_any_glob(path, globs):
    for g in globs:
        if fnmatch.fnmatchcase(path, g):
            return True

    return False


def get_files(dirs):
    for path in dirs:

        yield path, True, os.path.islink(path)

        for root, dirs, files in os.walk(path):
            for name in files:
                abs_name = os.path.join(root, name)
                yield abs_name, False, os.path.islink(abs_name)

            for name in dirs:
                abs_name = os.path.join(root, name)
                yield abs_name, True, os.path.islink(abs_name)


def get_db(db_filename):
    need_create_table = not os.path.exists(db_filename)

    conn = sqlite3.connect(db_filename)
    if need_create_table:
        create_table(conn)

    conn.row_factory = sqlite3.Row
    return conn


def encrypt_block(key, block_version, block_data):
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(key), modes.GCM(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_block = encryptor.update(block_data) + encryptor.finalize()

    if len(encryptor.tag) != 16:
        raise RuntimeError('BUG: GCM auth tag unexpected size. Failing.')

    return struct.pack('B', block_version) + iv + encryptor.tag + encrypted_block


def decrypt_block(key, block_version, encrypted_block):
    version = struct.unpack('B', encrypted_block[0:1])[0]
    iv = encrypted_block[1:17]
    auth_tag = encrypted_block[17:17 + 16]
    ciphertext = encrypted_block[17 + 16:]

    if version != block_version:
        raise RuntimeError('Trying to decrypt incorrect block format version. Block might be corrupt.')

    cipher = Cipher(algorithms.AES(key), modes.GCM(iv, tag=auth_tag), backend=default_backend())
    decryptor = cipher.decryptor()
    block_data = decryptor.update(ciphertext) + decryptor.finalize()

    return block_data


def get_sets_within_time(sets, date, time_delta):
    results = []
    for s in sets:
        if date <= sqlite_date_to_datetime(s['created']) <= date + time_delta:
            results.append(s)
    return results


def sqlite_date_to_datetime(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")


def sha1_file(filename):
    hasher = hashlib.sha1()
    with open(filename, 'rb') as f:
        while True:
            data = f.read(1024 * 8)
            if not data:
                break
            hasher.update(data)

    return hasher.hexdigest()


def read_range(file, start, size):
    """Reads a range from a file-like object"""

    max_read = 8 * 1024
    total_read = 0

    file.seek(start)
    while total_read != size:
        left = size - total_read
        if left < max_read:
            max_read = left

        data = file.read(max_read)
        total_read += len(data)
        yield data










def create_table(conn):
    sql = """CREATE TABLE file_set (
				id INTEGER PRIMARY KEY,
				created TEXT DEFAULT CURRENT_TIMESTAMP
			 );
		"""
    conn.execute(sql)

    sql = """CREATE TABLE file (
				id INTEGER PRIMARY KEY,
				name TEXT,
				dir_id INTEGER,
				uid INTEGER,
				gid INTEGER,
				permissions INTEGER,
				modified_time REAL,
				size INTEGER,
				hash BLOB,
				file_set_id INTEGER
			);
		"""
    conn.execute(sql)

    sql = "CREATE INDEX file_size_index ON file (size);"  # index for fast check to see if file changed
    conn.execute(sql)

    sql = """CREATE TABLE simlink (
				id INTEGER PRIMARY KEY,
				name TEXT,
				dest TEXT,
				file_set_id INTEGER
			);
		"""
    conn.execute(sql)

    sql = """CREATE TABLE dir (
				id INTEGER PRIMARY KEY,
				name TEXT,
				uid INTEGER,
				gid INTEGER,
				permissions INTEGER,
				file_set_id INTEGER
			);
		"""
    conn.execute(sql)

    sql = """CREATE TABLE file_map (
			id INTEGER PRIMARY KEY,
			file_id INTEGER,
			block_id INTEGER
		);
	"""
    conn.execute(sql)

    sql = "CREATE INDEX map_file_id_index ON file_map (file_id);"
    conn.execute(sql)

    sql = "CREATE INDEX map_block_id_index ON file_map (block_id);"
    conn.execute(sql)

    sql = """CREATE TABLE block (
				id INTEGER PRIMARY KEY,
				hash BLOB
			 );
		"""
    conn.execute(sql)

    sql = "CREATE INDEX hash_index ON block (hash);"
    conn.execute(sql)



class BlockWriter(threading.Thread):
    def __init__(self, key, block_format_version, verbose=False):
        threading.Thread.__init__(self)

        self.verbose = verbose

        self.queue = queue.Queue()
        self.key = key
        self.block_format_version = block_format_version
        self.file_blocks_counter = {}

    def put(self, out_path, download_future, file_offset, total_blocks, expected_hash, permissions):
        self.queue.put((out_path, download_future, file_offset, total_blocks, expected_hash, permissions))

    def run(self):
        while True:
            task = self.queue.get()

            if task == 'shutdown':
                if self.verbose:
                    print('Block writer thread exiting')
                break

            fd = task[0]
            future = task[1]
            offset = task[2]
            total_blocks = task[3]
            expected_hash = task[4]
            permissions = task[5]

            future.lock.acquire()

            encrypted = future.response

            compressed = decrypt_block(self.key, self.block_format_version, encrypted)
            block_data = gzip.decompress(compressed)

            fd.seek(offset)
            fd.write(block_data)

            if fd in self.file_blocks_counter:
                self.file_blocks_counter[fd] += 1
            else:
                self.file_blocks_counter[fd] = 1

            if self.file_blocks_counter[fd] == total_blocks:
                self._complete_file(fd, expected_hash, permissions)

    def _complete_file(self, fd, expected_hash, perms):

        fd.close()

        file_hasher = hashlib.sha256()
        with open(fd.name, 'rb') as fd:
            while True:
                data = fd.read(16 * 1024)
                if len(data) == 0:
                    break
                file_hasher.update(data)

        set_file_attributes(fd.name, uid=perms[0], gid=perms[1], permissions=perms[2], verbose=self.verbose)

        print('FILE', fd.name)

        if file_hasher.digest()[16:] != expected_hash:
            raise RuntimeError('File corrupt:', fd.name)

    def shutdown(self):
        self.queue.put('shutdown')
