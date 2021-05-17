import sqlite3, os


SQL_COPY_FILE_HASH = 'UPDATE file SET hash = (SELECT hash FROM file WHERE id = ?) WHERE id = ?'
SQL_DELETE_SET = 'DELETE FROM backup_set WHERE id = ?'
SQL_DELETE_FILE = 'DELETE FROM file WHERE id = ?'
SQL_DELETE_SYMLINK = 'DELETE FROM symlink WHERE id = ?'
SQL_DELETE_DIR = 'DELETE FROM dir WHERE id = ?'
SQL_DELETE_FILE_BLOCK_MAPS = 'DELETE FROM file_block_map WHERE id IN (%s)'
SQL_DELETE_BLOCKS = 'DELETE FROM block WHERE id IN (%s)'
SQL_DELETE_UNUSED_SET_FILE_MAPS = 'DELETE FROM set_file_map WHERE set_id NOT IN (SELECT id FROM backup_set)'
SQL_DELETE_UNUSED_SET_SYMLINK_MAPS = 'DELETE FROM set_symlink_map WHERE set_id NOT IN (SELECT id FROM backup_set)'
SQL_DELETE_UNUSED_SET_DIR_MAPS = 'DELETE FROM set_dir_map WHERE set_id NOT IN (SELECT id FROM backup_set)'
SQL_INSERT_FILE_MAP = 'INSERT INTO file_block_map (file_id, block_id) VALUES (?,?)'
SQL_INSERT_FILE_SET = 'INSERT INTO backup_set DEFAULT VALUES'
SQL_SELECT_BLOCK_BY_ID = 'SELECT id FROM block WHERE id = ?'
SQL_SELECT_BLOCKS = 'SELECT id FROM block'
SQL_SELECT_DIR_BY_NAME = 'SELECT id FROM dir WHERE name = ?'
SQL_SELECT_FILE_BLOCKS = 'SELECT block_id FROM file_block_map WHERE file_id = ? ORDER BY id ASC'
SQL_SELECT_SETS = 'SELECT * FROM backup_set ORDER BY created DESC'
SQL_SELECT_FILE_BY_BLOCK = 'SELECT file.* FROM file, file_block_map, block WHERE file.id = file_block_map.file_id AND file_block_map.block_id = block.id AND block.id = ?'
SQL_SELECT_BLOCK_AND_COUNT_BY_FILE = 'SELECT m1.id, m1.block_id, (SELECT COUNT(id) FROM file_block_map as m2 WHERE block_id = m1.block_id AND m2.file_id != ?) as other_count FROM file_block_map as m1 WHERE file_id = ?'
SQL_SELECT_OTHER_SET_FILE_MAP_COUNT = 'SELECT COUNT(id) as count FROM set_file_map WHERE set_id != ? AND file_id = ?'
SQL_SELECT_OTHER_SET_SYMLINK_MAP_COUNT = 'SELECT COUNT(id) as count FROM set_symlink_map WHERE set_id != ? AND symlink_id = ?'
SQL_SELECT_OTHER_SET_DIR_MAP_COUNT = 'SELECT COUNT(id) as count FROM set_dir_map WHERE set_id != ? AND dir_id = ?'
SQL_UPDATE_FILE_HASH = 'UPDATE file SET hash = ? WHERE id = ?'


def get_db(db_filename):
    need_create_table = not os.path.exists(db_filename)

    conn = sqlite3.connect(db_filename, isolation_level='DEFERRED')
    if need_create_table:
        create_table(conn)

    conn.row_factory = sqlite3.Row
    return conn

def optimise_db(db_filename):
    conn = sqlite3.connect(db_filename, isolation_level=None)
    conn.execute('VACUUM')

def create_table(conn):
    sql = """CREATE TABLE backup_set (
				id INTEGER PRIMARY KEY,
				created TEXT DEFAULT CURRENT_TIMESTAMP
			 );
		"""
    conn.execute(sql)

    sql = """CREATE TABLE file (
				id INTEGER PRIMARY KEY,
				name TEXT,
				set_dir_id INTEGER,
				uid INTEGER,
				gid INTEGER,
				permissions INTEGER,
				modified_time REAL,
				size INTEGER,
				hash BLOB
			);
		"""
    conn.execute(sql)

    sql = "CREATE INDEX file_size_and_modified_time_index ON file (size, modified_time);"  # index for fast check to see if file changed
    conn.execute(sql)

    sql = """CREATE TABLE symlink (
				id INTEGER PRIMARY KEY,
				name TEXT,
				dest TEXT
			);
		"""
    conn.execute(sql)

    sql = "CREATE INDEX symlink_name_index ON symlink (name);"
    conn.execute(sql)

    sql = """CREATE TABLE dir (
				id INTEGER PRIMARY KEY,
				name TEXT,
				uid INTEGER,
				gid INTEGER,
				permissions INTEGER
			);
		"""
    conn.execute(sql)

    sql = "CREATE INDEX dir_name_index ON dir (name);"
    conn.execute(sql)



    sql = """CREATE TABLE file_block_map (
			id INTEGER PRIMARY KEY,
			file_id INTEGER,
			block_id INTEGER
		);
	"""
    conn.execute(sql)


    sql = """CREATE TABLE set_file_map (
    			id INTEGER PRIMARY KEY,
    			set_id INTEGER,
    			file_id INTEGER
    		);
    	"""
    conn.execute(sql)

    sql = "CREATE INDEX set_file_id_index ON set_file_map (file_id);"
    conn.execute(sql)


    sql = """CREATE TABLE set_dir_map (
    			id INTEGER PRIMARY KEY,
    			set_id INTEGER,
    			dir_id INTEGER
    		);
    	"""
    conn.execute(sql)

    sql = "CREATE INDEX set_dir_id_index ON set_dir_map (dir_id);"
    conn.execute(sql)


    sql = """CREATE TABLE set_symlink_map (
    			id INTEGER PRIMARY KEY,
    			set_id INTEGER,
    			symlink_id INTEGER
    		);
    	"""
    conn.execute(sql)

    sql = "CREATE INDEX set_symlink_id_index ON set_symlink_map (symlink_id);"
    conn.execute(sql)


    sql = "CREATE INDEX map_file_id_index ON file_block_map (file_id);"
    conn.execute(sql)

    sql = "CREATE INDEX map_block_id_index ON file_block_map (block_id);"
    conn.execute(sql)

    sql = """CREATE TABLE block (
				id INTEGER PRIMARY KEY,
				hash BLOB
			 );
		"""
    conn.execute(sql)

    sql = "CREATE INDEX hash_index ON block (hash);"
    conn.execute(sql)


def handle_symlink(cur, set_id, name, dest):

    sql = 'SELECT id FROM symlink WHERE name = ? AND dest = ?'
    result = cur.execute(sql, (name, dest)).fetchone()
    if result:
        symlink_id = result['id']
    else:
        sql = 'INSERT INTO symlink (name, dest) VALUES (?,?)'
        cur.execute(sql, (name, dest))
        symlink_id = cur.lastrowid

    sql = 'INSERT INTO set_symlink_map (set_id, symlink_id) VALUES (?,?)'
    cur.execute(sql, (set_id, symlink_id))


def handle_dir(cur, name, set_id, st):

    sql = 'SELECT id FROM dir WHERE name = ? AND uid = ? AND gid = ? AND permissions = ?'
    result = cur.execute(sql, (name, st.st_uid, st.st_gid, st.st_mode)).fetchone()
    if result:
        dir_id = result['id']
    else:
        sql = 'INSERT INTO dir (name, uid, gid, permissions) VALUES (?,?,?,?)'
        cur.execute(sql, (name, st.st_uid, st.st_gid, st.st_mode))
        dir_id = cur.lastrowid

    sql = 'INSERT INTO set_dir_map (set_id, dir_id) VALUES (?,?)'
    cur.execute(sql, (set_id, dir_id))

    return cur.lastrowid


def get_prev_file(cur, file_name, dir_name, st):


    sql =  """SELECT file.id, file.uid, file.gid, file.permissions FROM file, set_dir_map, dir
                            WHERE file.name = ? AND file.modified_time = ? AND file.size = ?
                            AND file.uid = ? AND file.gid = ? AND file.permissions = ?

                            AND set_dir_map.id = file.set_dir_id AND set_dir_map.dir_id = dir.id
                            AND dir.name = ?"""

    result = cur.execute(sql,  (file_name, st.st_mtime, st.st_size, st.st_uid, st.st_gid, st.st_mode, dir_name)).fetchone()
    if result:
        return result
    else:
        return None


def insert_file_for_set(cur, set_id, file_name, set_dir_id, st):

    sql = 'INSERT INTO file (name, set_dir_id, uid, gid, permissions, modified_time, size) VALUES (?,?,?,?,?,?,?)'
    cur.execute(sql, (file_name, set_dir_id, st.st_uid, st.st_gid, st.st_mode, st.st_mtime, st.st_size))
    file_id = cur.lastrowid

    map_existing_file_to_set(cur, set_id, cur.lastrowid)

    return file_id


def map_existing_file_to_set(cur, set_id, file_id):

    sql = 'INSERT INTO set_file_map (set_id, file_id) VALUES (?,?)'
    cur.execute(sql, (set_id, file_id))


def find_or_insert_block(cur, block_hash):

    sql = 'SELECT id FROM block WHERE hash = ?'
    result = cur.execute(sql, (block_hash,)).fetchone()

    if result:
        return result['id'], False #isn't new

    sql = 'INSERT INTO block (hash) VALUES (?)'
    cur.execute(sql, (block_hash,))

    return cur.lastrowid, True #is new

def get_results_for_set(conn, backup_set_id):

    sql = 'SELECT dir.* FROM dir, set_dir_map WHERE set_dir_map.set_id = ? AND dir.id = set_dir_map.dir_id'
    dir_results = conn.execute(sql, (backup_set_id,)).fetchall()

    sql = 'SELECT symlink.* FROM symlink, set_symlink_map WHERE set_symlink_map.set_id = ? AND symlink.id = set_symlink_map.symlink_id'
    symlink_results = conn.execute(sql, (backup_set_id,)).fetchall()

    sql = """SELECT file.*, dir.name as dir_name FROM file, set_file_map, set_dir_map, dir
                    WHERE set_file_map.set_id = ? AND file.id = set_file_map.file_id
                    AND file.set_dir_id = set_dir_map.id AND dir.id = set_dir_map.dir_id"""
    file_results = conn.execute(sql, (backup_set_id,)).fetchall()

    return dir_results, symlink_results, file_results




