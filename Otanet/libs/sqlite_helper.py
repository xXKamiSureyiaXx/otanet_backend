import sqlite3
import boto3
import threading
from datetime import datetime

class SQLiteHelper:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = 'otanet-manga-devo'
        # Each thread should create its own connection
        self.conn = None
        self.cursor = None
        self._init_connection()
    
    def _init_connection(self):
        """Initialize connection for this thread"""
        self.conn = sqlite3.connect('otanet_devo.db', check_same_thread=False, timeout=30.0)
        # Enable WAL mode for better concurrent writes
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.cursor = self.conn.cursor()
    
    def should_insert(self):
        should_insert = True

        if self.cursor.fetchone()[0] > 0:
            print("Data exists in Database")
            should_insert = False

        return should_insert

    def create_metadata_table(self, table_name):
        """Create the manga metadata table if it doesn't exist"""
        try:
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
            with self._lock:
                self.cursor.execute(create_table_query)
                self.conn.commit()
            print(f"Table {table_name} created or already exists")
        except sqlite3.Error as e:
            print(f"Error creating table {table_name}: {e}")
            self.conn.rollback()

    def create_page_urls_table(self, manga_id):
        """Create a page URLs table for a specific manga using manga_id as the table name"""

        manga_id = manga_id.replace("-", "_")  # SQLite table names cannot have hyphens
        try:
            create_table_query = f"""CREATE TABLE IF NOT EXISTS [{manga_id}] (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    manga_name TEXT NOT NULL,
                    chapter_num TEXT NOT NULL,
                    page_number TEXT NOT NULL,
                    page_url TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                );"""
            with self._lock:
                self.cursor.execute(create_table_query)
                self.conn.commit()
            print(f"Table [{manga_id}] created or already exists")
        except sqlite3.Error as e:
            print(f"Error creating table [{manga_id}]: {e}")
            self.conn.rollback()

    def insert_manga_metadata(self, table_name, manga):
        check_hash_query = f"SELECT COUNT(*) FROM {table_name} WHERE hash = ?"
    
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.cursor.execute(check_hash_query, (manga.get_id(),))
                should_insert = self.should_insert()

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
                            datetime.now())
                
                    if manga.get_latest_chapter() == 0:
                        return
                
                    print("Query: ", insert_metadata_query, insert_data)
                    self.cursor.execute(insert_metadata_query, insert_data)
                    self.conn.commit()
                    print(f"Data inserted successfully: {manga.get_id()}")
                else:
                    check_latest_chapter = f"SELECT latest_chapter FROM {table_name} WHERE hash = '{manga.get_id()}'"
                    self.cursor.execute(check_latest_chapter)
                    if float(self.cursor.fetchone()[0]) > float(manga.get_latest_chapter()):
                        update_latest_chapter_query = f"""
                            UPDATE {table_name}
                            SET latest_chapter = {manga.get_latest_chapter()},
                                cover_img = '{manga.get_cover_img()}',
                                time = '{datetime.now()}'
                            WHERE hash = '{manga.get_id()}';"""
                        print("Query: ", update_latest_chapter_query)
                        self.cursor.execute(update_latest_chapter_query)
                        self.conn.commit()
                        print(f"Successfully updated latest chapter for: {manga.get_id()}")
                break  # Success, exit retry loop
            
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    print(f"Database locked, retrying... (attempt {attempt + 1}/{max_retries})")
                    import time
                    time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    print(f"Error executing database operation: {e}")
                    self.conn.rollback()
                    raise
            except sqlite3.Error as e:
                print(f"Error executing database operation: {e}")
                self.conn.rollback()
                raise
        self.data_to_s3()

    def data_to_s3(self):
        """Upload database to S3 - should be called periodically, not after every insert"""
        try:
            self.s3_client.upload_file(f"otanet_devo.db", self.bucket_name, "database/otanet_devo.db")
            print("Database uploaded to S3 successfully")
        except Exception as e:
            print(f"Error uploading to S3: {e}")

    def store_page_url(self, manga_id, manga_name, chapter_num, page_number, page_url):
        """Store page URL information to the manga-specific table"""
        manga_id = manga_id.replace("-", "_")
    
        max_retries = 3
        for attempt in range(max_retries):
            try:
                insert_page_query = f"INSERT INTO [{manga_id}] (manga_name, chapter_num, page_number, page_url, timestamp) VALUES (?, ?, ?, ?, ?);"
                insert_data = (manga_name, chapter_num, page_number, page_url, datetime.now())
                print(f"Query: {insert_page_query}, Data: {insert_data}")
                self.cursor.execute(insert_page_query, insert_data)
                self.conn.commit()
                print(f"Page URL stored successfully in table [{manga_id}]: {manga_name} - Chapter {chapter_num} - Page {page_number}")
                break  # Success
            
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    print(f"Database locked, retrying... (attempt {attempt + 1}/{max_retries})")
                    import time
                    time.sleep(0.1 * (attempt + 1))
                    continue
                else:
                    print(f"Error storing page URL to database: {e}")
                    self.conn.rollback()
                    raise
            except sqlite3.Error as e:
                print(f"Error storing page URL to database: {e}")
                self.conn.rollback()
                raise
        self.data_to_s3()

    def disconnect(self):
        self.conn.close()