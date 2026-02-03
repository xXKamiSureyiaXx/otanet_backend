import sqlite3
import boto3
import time
from datetime import datetime

class SQLiteHelper:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = 'otanet-manga-devo'
        self.db_path = 'otanet_devo.db'
        # Don't create connection in __init__, create per-operation
        
    def _get_connection(self):
        """Get a new connection for each operation"""
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.execute('PRAGMA journal_mode=WAL')
        return conn
    
    def should_insert(self, cursor):
        should_insert = True

        if cursor.fetchone()[0] > 0:
            print("Data exists in Database")
            should_insert = False

        return should_insert

    def create_metadata_table(self, table_name):
        """Create the manga metadata table if it doesn't exist"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT,
                    cover_img TEXT,
                    tags TEXT,
                    hash TEXT UNIQUE NOT NULL,
                    latest_chapter REAL,
                    time DATETIME DEFAULT CURRENT_TIMESTAMP
                );"""
            cursor.execute(create_table_query)
            conn.close()
            print(f"Table {table_name} created or already exists")
        except sqlite3.Error as e:
            print(f"Error creating table {table_name}: {e}")

    def create_page_urls_table(self, manga_id):
        """Create a page URLs table for a specific manga using manga_id as the table name"""
        manga_id = manga_id.replace("-", "_")  # SQLite table names cannot have hyphens
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            create_table_query = f"""CREATE TABLE IF NOT EXISTS [{manga_id}] (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    manga_name TEXT NOT NULL,
                    chapter_num TEXT NOT NULL,
                    page_number TEXT NOT NULL,
                    page_url TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chapter_num, page_number)
                );"""
            cursor.execute(create_table_query)
            conn.close()
            print(f"Table [{manga_id}] created or already exists")
        except sqlite3.Error as e:
            print(f"Error creating table [{manga_id}]: {e}")

    def get_manga_latest_chapter(self, table_name, manga_hash):
        """
        OPTIMIZATION: Get the latest chapter number for a manga from the database
        Returns None if manga doesn't exist
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                query = f"SELECT MAX(DISTINCT chapter_num) FROM [{table_name}]"
                cursor.execute(query)
                result = cursor.fetchone()
                
                conn.close()
                
                if result:
                    return float(result[0]) if result[0] else None
                return None
                
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    print(f"Database locked, retrying... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(0.1 * (attempt + 1))
                    continue
                else:
                    print(f"Error getting manga latest chapter: {e}")
                    return None
            except sqlite3.Error as e:
                print(f"Error getting manga latest chapter: {e}")
                return None

    def get_existing_chapter_pages(self, manga_id, chapter_num):
        """
        NEW: Get a set of page numbers that already exist for a specific chapter
        Returns empty set if no pages exist
        This allows us to download only missing pages instead of skipping entire chapter
        """
        manga_id_normalized = manga_id.replace("-", "_")
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Check if table exists
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?
                """, (manga_id_normalized,))
                
                if not cursor.fetchone():
                    conn.close()
                    return set()
                
                # Get page numbers for this specific chapter
                query = f"SELECT page_number FROM [{manga_id_normalized}] WHERE chapter_num = ?"
                cursor.execute(query, (str(chapter_num),))
                pages = {row[0] for row in cursor.fetchall()}
                
                conn.close()
                return pages
                
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    print(f"Database locked, retrying... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(0.1 * (attempt + 1))
                    continue
                else:
                    print(f"Error getting existing chapter pages: {e}")
                    return set()
            except sqlite3.Error as e:
                print(f"Error getting existing chapter pages: {e}")
                return set()

    def get_chapters_with_status(self, manga_id):
        """
        NEW: Get dictionary of chapters with their completion status
        Returns: {
            'chapter_num': {
                'page_count': int,
                'is_complete': bool (if we know total pages),
                'pages': set of page numbers
            }
        }
        """
        manga_id_normalized = manga_id.replace("-", "_")
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Check if table exists
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?
                """, (manga_id_normalized,))
                
                if not cursor.fetchone():
                    conn.close()
                    return {}
                
                # Get all chapters with their pages
                query = f"""
                    SELECT chapter_num, page_number 
                    FROM [{manga_id_normalized}]
                    ORDER BY chapter_num, page_number
                """
                cursor.execute(query)
                
                chapters = {}
                for row in cursor.fetchall():
                    chapter_num = row[0]
                    page_number = row[1]
                    
                    if chapter_num not in chapters:
                        chapters[chapter_num] = {
                            'page_count': 0,
                            'pages': set()
                        }
                    
                    chapters[chapter_num]['pages'].add(page_number)
                    chapters[chapter_num]['page_count'] += 1
                
                conn.close()
                return chapters
                
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    print(f"Database locked, retrying... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(0.1 * (attempt + 1))
                    continue
                else:
                    print(f"Error getting chapters with status: {e}")
                    return {}
            except sqlite3.Error as e:
                print(f"Error getting chapters with status: {e}")
                return {}

    def insert_manga_metadata(self, table_name, manga):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                check_hash_query = f"SELECT COUNT(*) FROM {table_name} WHERE hash = ?"
                cursor.execute(check_hash_query, (manga.get_id(),))
                
                if cursor.fetchone()[0] > 0:
                    should_insert = False
                else:
                    should_insert = True

                if should_insert:
                    insert_metadata_query = f"""INSERT INTO {table_name} (title, description, tags, hash, latest_chapter, cover_img, time) 
                        VALUES (?,?,?,?,?,?,?);"""

                    insert_data = (
                        manga.get_title(), 
                        manga.get_description(),
                        str(manga.get_tags()),
                        manga.get_id(),
                        manga.get_latest_chapter(),
                        manga.get_cover_img(),
                        datetime.now().isoformat())
                    
                    if manga.get_latest_chapter() == 0:
                        conn.close()
                        return
                    
                    print("Query: ", insert_metadata_query, insert_data)
                    cursor.execute(insert_metadata_query, insert_data)
                    print(f"Data inserted successfully: {manga.get_id()}")
                else:
                    check_latest_chapter = f"SELECT latest_chapter FROM {table_name} WHERE hash = ?"
                    cursor.execute(check_latest_chapter, (manga.get_id(),))
                    result = cursor.fetchone()
                    if result and float(result[0]) < float(manga.get_latest_chapter()):
                        update_latest_chapter_query = f"""
                            UPDATE {table_name}
                            SET latest_chapter = ?,
                                cover_img = ?,
                                time = ?
                            WHERE hash = ?;"""
                        print("Query: ", update_latest_chapter_query)
                        cursor.execute(update_latest_chapter_query, 
                                     (manga.get_latest_chapter(), manga.get_cover_img(), 
                                      datetime.now().isoformat(), manga.get_id()))
                        print(f"Successfully updated latest chapter for: {manga.get_id()}")
                
                conn.close()
                break
                
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    print(f"Database locked, retrying... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(0.1 * (attempt + 1))
                    continue
                else:
                    print(f"Error executing database operation: {e}")
                    raise
            except sqlite3.Error as e:
                print(f"Error executing database operation: {e}")
                raise

    def data_to_s3(self):
        """Upload database to S3"""
        try:
            self.s3_client.upload_file(self.db_path, self.bucket_name, "database/otanet_devo.db")
            print("Database uploaded to S3 successfully")
        except Exception as e:
            print(f"Error uploading to S3: {e}")

    def store_page_url(self, manga_id, manga_name, chapter_num, page_number, page_url):
        """Store page URL information to the manga-specific table"""
        manga_id = manga_id.replace("-", "_")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Simplified insert with UNIQUE constraint handling
                insert_page_query = f"""INSERT OR IGNORE INTO [{manga_id}] 
                    (manga_name, chapter_num, page_number, page_url, timestamp)
                    VALUES (?, ?, ?, ?, ?);"""
                    
                insert_data = (
                    str(manga_name), 
                    str(chapter_num), 
                    str(page_number), 
                    str(page_url), 
                    datetime.now().isoformat()
                )
                
                cursor.execute(insert_page_query, insert_data)
                
                if cursor.rowcount > 0:
                    print(f"Page URL stored successfully in table [{manga_id}]: {manga_name} - Chapter {chapter_num} - Page {page_number}")
                else:
                    print(f"Page URL already exists in table [{manga_id}]: {manga_name} - Chapter {chapter_num} - Page {page_number}")
                
                conn.close()
                break
                
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    print(f"Database locked, retrying... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(0.1 * (attempt + 1))
                    continue
                else:
                    print(f"Error storing page URL to database: {e}")
                    raise
            except sqlite3.Error as e:
                print(f"Error storing page URL to database: {e}")
                raise

    def disconnect(self):
        # No persistent connection to close
        pass
