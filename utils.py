import os, stat, struct, threading, gzip, queue, hashlib, fnmatch
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

                is_symlink = os.path.islink(abs_name)

                #skip if the file is a socket
                if not is_symlink and stat.S_ISSOCK(os.stat(abs_name).st_mode):
                    continue

                yield abs_name, False, is_symlink


            for name in dirs:
                abs_name = os.path.join(root, name)
                yield abs_name, True, os.path.islink(abs_name)


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

def get_files_in_b2(b2_threaded):

    future = b2_threaded.list_files()
    future.lock.acquire()

    known_b2_files = {}
    for b2_file in future.response:
        known_b2_files.setdefault(b2_file.name, []).append(b2_file)

    return known_b2_files

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


def format_bytes(size): # based on https://stackoverflow.com/a/49361727
    power = 1024
    n = 0
    power_labels = {0 : 'bytes', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size > power:
        size /= power
        n += 1
    return size, power_labels[n]


class BlockWriter(threading.Thread):
    def __init__(self, key, block_format_version, verbose=False):
        threading.Thread.__init__(self)

        self.verbose = verbose

        self.queue = queue.Queue(2000) #2000 * 256KB blocks == 500MB max
        self.key = key
        self.block_format_version = block_format_version
        self.file_blocks_counter = {}

    def put(self, out_path, download_future, file_offset, total_blocks, expected_hash, permissions):
        self.queue.put((out_path, download_future, file_offset, total_blocks, expected_hash, permissions))

    def run(self):
        while True:
            task = self.queue.get()

            if task == 'shutdown':
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


class StatTracker:
    total_files = 0
    total_bytes_processed = 0
    total_bytes_uploaded_uncompressed = 0
    total_bytes_uploaded_compressed = 0

    def inc_file(self):
        self.total_files += 1

    def inc_bytes_processed(self, amount):
        self.total_bytes_processed += amount

    def inc_uploaded_uncompressed(self, amount):
        self.total_bytes_uploaded_uncompressed += amount

    def inc_uploaded_compressed(self, amount):
        self.total_bytes_uploaded_compressed += amount


    def print_stats(self):
        processed = format_bytes(self.total_bytes_processed)
        uncompressed = format_bytes(self.total_bytes_uploaded_uncompressed)
        compressed = format_bytes(self.total_bytes_uploaded_compressed)

        stats = "Backup Statistics\n"
        stats += f"\tTotal Files: {self.total_files:,}\n"
        stats += "\tData Processed: {:.2f}{}\n".format(processed[0], processed[1])
        stats += "\tData Changed: {:.2f}{}\n".format(uncompressed[0], uncompressed[1])
        stats += "\tCompressed Data Uploaded: {:.2f}{}\n".format(compressed[0], compressed[1])
        print(stats)