import sqlite3
import boto3
from datetime import datetime

class SQLiteHelper:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = 'otanet-manga-devo'
        self.conn = sqlite3.connect('otanet_devo.db')
        self.cursor = self.conn.cursor()
    
    def should_insert(self):
        should_insert = True

        if self.cursor.fetchone()[0] > 0:
            print("Data exists in Database")
            should_insert = False

        return should_insert

    def insert_manga_metadata(self, table_name, manga):
        check_hash_query = f"SELECT COUNT(*) FROM {table_name} WHERE hash = ?"
        self.cursor.execute(check_hash_query, (manga.get_id(),))
        should_insert = self.should_insert()

        if should_insert:
            insert_metadata_query = f"""
                    INSERT INTO {table_name} (title, description, tags, hash, latest_chapter, time) 
                    VALUES (?,?,?,?,?,?);"""

            insert_data = (
                    manga.get_title(), 
                    manga.get_description(),
                    str(manga.get_tags()),
                    manga.get_id(),
                    manga.get_latest_chapter(),
                    datetime.now())
            
            if manga.get_latest_chapter() == 0:
                return
            
            try:
                print("Query: ", insert_metadata_query, insert_data)
                self.cursor.execute(insert_metadata_query, insert_data)
                self.conn.commit()
            except sqlite3.Error as e:
                print(f"Error executing INSERT statement: {e}")
                self.conn.rollback()
            print(f"Data inserted successfully: {manga.get_id()}")
        else:
            check_latest_chapter = f"SELECT latest_chapter FROM {table_name} WHERE hash = {manga.get_id()}"
            self.cursor.execute(check_latest_chapter)
            print('Cursor: ', self.cursor.fetchone()[0])
            if int(self.cursor.fetchone()[0]) > manga.get_latest_chapter():
                update_latest_chapter_query = f"""
                    UPDATE {table_name}
                    SET latest_chapter = {manga.get_latest_chapter()}
                    SET time = {datetime.now()}
                    WHERE hash = '{manga.get_id()}';"""
                try:
                    print("Query: ", update_latest_chapter_query)
                    self.cursor.execute(update_latest_chapter_query)
                    self.conn.commit()
                except sqlite3.Error as e:
                    print(f"Error executing UPDATE statement: {e}")
                    self.conn.rollback()
                print(f"Successfully updated latest chapter for: {manga.get_id()}")
        self.data_to_s3()

    def data_to_s3(self):
        self.s3_client.upload_file(f"otanet_devo.db", self.bucket_name, "database/otanet_devo.db")

    def disconnect(self):
        self.conn.close()
        