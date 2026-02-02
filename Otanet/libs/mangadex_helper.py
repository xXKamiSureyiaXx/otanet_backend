import requests
import os
import re
import boto3
import botocore.exceptions
import string
import time
import threading
from utils import Utils
from sqlite_helper import SQLiteHelper
import random


class MangaDexHelper:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.s3_resource.Bucket('otanet-manga-devo')
        self.utils = Utils()
        self.db = SQLiteHelper()
        self.bucket_name = 'otanet-manga-devo'
        self.base_url = "https://api.mangadex.org"
        self.pagnation_limit = 25
        self.languages = ["en"]
        self.root_directory = os.getcwd()
    
    def get_recent_manga(self, offset):
        base_response = requests.get(
            f"{self.base_url}/manga",
            params={"limit": self.pagnation_limit,
                    "offset": offset}
        )

        manga_list = []
        for manga in base_response.json()["data"]:
            try: 
                manga_relationships = next((obj for obj in manga["relationships"] if obj["type"] == "cover_art"), False)
                cover_id = self.get_manga_cover_id(manga_relationships)
      
                tags = []
                for tag in manga["attributes"]["tags"]:
                    tags.append(self.utils.normalize_database_text(tag["attributes"]["name"]["en"]))
                
                manga_list.append({
                    'id': manga['id'],
                    'title': self.utils.normalize_database_text(manga["attributes"]["title"]["en"]),
                    'description': self.utils.normalize_database_text(manga["attributes"]["description"]["en"]),
                    'cover_img': f"https://uploads.mangadex.org/covers/{manga['id']}/{cover_id}",
                    'tags': tags
                })
            except:
                continue
        return manga_list
    
    def set_latest_chapters(self, manga):
        """
        OPTIMIZATION: Check if manga exists in database first
        If it exists and has chapters, only fetch new chapters
        """
        # Check if manga exists and get its latest chapter from DB
        existing_chapter = self.db.get_manga_latest_chapter('manga_metadata', manga.get_id())
        
        all_chapters = []
        offset = 0
        
        # Fetch all chapters (we need to know the absolute latest)
        while True:
            response = requests.get(
                f"{self.base_url}/manga/{manga.get_id()}/feed",
                params={"translatedLanguage[]": self.languages, "offset": offset, "limit": 100},
            )

            chapters = response.json().get("data", [])
            all_chapters.extend(chapters)

            print(f"Fetched {len(chapters)} chapters for manga {manga.get_id()} at offset {offset}")

            if len(chapters) < 100:
                break
            offset += len(chapters)

        if not all_chapters:
            print(f"No chapters found for manga {manga.get_id()}")
            return False

        class MockResponse:
            def __init__(self, data):
                self._data = data

            def json(self):
                return {"data": self._data}

        combined_response = MockResponse(all_chapters)
        manga.set_chapters(combined_response)

        should_download = manga.set_latest_chapter()
        
        # OPTIMIZATION: Only download if there are new chapters
        if existing_chapter is not None:
            if manga.get_latest_chapter() <= existing_chapter:
                print(f"No new chapters for manga {manga.get_id()} (Latest: {manga.get_latest_chapter()}, DB: {existing_chapter})")
                return False
            else:
                print(f"New chapters available for manga {manga.get_id()} (Latest: {manga.get_latest_chapter()}, DB: {existing_chapter})")
        
        return should_download
        
    def download_chapters(self, manga):
        """
        OPTIMIZATION: Check which chapters already exist in the database
        before attempting to download
        """
        # Get list of chapters that already exist in the database
        existing_chapters = self.db.get_existing_chapters(manga.get_id())
        print(f"Found {len(existing_chapters)} existing chapters in database for manga {manga.get_id()}")
        
        for chapter in manga.get_chapters():
            chapter_num = chapter["attributes"]["chapter"].replace('.', '_')
            
            # OPTIMIZATION: Skip if chapter already exists in database
            if chapter_num in existing_chapters:
                print(f"Chapter {chapter_num} already exists in database for {manga.get_id()}, skipping...")
                continue
            
            time.sleep(random.uniform(5, 20.0))  # Stagger chapter downloads
            print(f"Request for {manga.get_id()} chapter {chapter_num}")

            title = self.utils.normalize_s3_text(manga.get_title())
            chapter_path = f"{manga.get_id()}/chapter_{chapter_num}"
            base_key = f"{title}/chapter_{chapter_num}"
            
            print("Storing Page URLs to Database")
            self.store_page_url_to_database(chapter['id'], title, chapter_num, manga.get_id())
            
            os.chdir(self.root_directory)

    def data_to_s3(self):
        """Upload database to S3 - only call this periodically, not after every manga"""
        print("Updating Database")
        self.s3_client.upload_file(f"otanet_devo.db", self.bucket_name, "database/otanet_devo.db")

    def get_requested_manga(self, manga_id):
        manga = requests.get(
            f"{self.base_url}/manga/{manga_id}",
                params={"translatedLanguage[]": self.languages},
            ).json()
        
        try:
            manga_relationships = next((obj for obj in manga['data']["relationships"] if obj["type"] == "cover_art"), False)
            cover_id = self.get_manga_cover_id(manga_relationships)

            tags = []
            for tag in manga["data"]["attributes"]["tags"]:
                tags.append(self.utils.normalize_database_text(tag["attributes"]["name"]["en"]))

            dict = {
                    'id': manga['data']['id'],
                    'title': self.utils.normalize_database_text(manga["data"]["attributes"]["title"]["en"]),
                    'description': self.utils.normalize_database_text(manga["data"]["attributes"]["description"]["en"]),
                    'cover_img': f"https://uploads.mangadex.org/covers/{manga['data']['id']}/{cover_id}",
                    'tags': tags
                }
            return dict
        except Exception as e:
            print(f"Manga not Found: {e}")
        
    def get_manga_cover_id(self, manga_relationships):
        cover_response = requests.get(f"{self.base_url}/cover/{manga_relationships['id']}")
        return cover_response.json()["data"]["attributes"]["fileName"]
    
    def download_cover(self, path, title, cover):
        s3_obj_title_key = f"{title}/0_title/cover_img"
        directory = f"{path}/title"
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_obj_title_key)
        except:
            with open(directory, mode="wb") as f:
                img_data = requests.get(cover).content
                f.write(img_data)
                self.s3_client.upload_file(directory, self.bucket_name, s3_obj_title_key, ExtraArgs={'ContentType': "image/png"})
            try:
                os.remove(directory)
            except:
                print(f"Failed to remove {path}/title directory")
    
    def store_page_url_to_database(self, chapter_id, title, chapter_num, manga_id):
        """
        OPTIMIZATION: Check if pages already exist before making API call
        """
        print("Checking if chapter pages already exist in database...")
        
        # Check if this chapter already has pages in the database
        if self.db.chapter_pages_exist(manga_id, chapter_num):
            print(f"Chapter {chapter_num} pages already exist in database, skipping API call")
            return
        
        print("Fetching page URLs from MangaDex API...")

        retries = 0
        while retries <= 10:
            chapter_resp = requests.get(f"{self.base_url}/at-home/server/{chapter_id}")
            resp_json = chapter_resp.json()

            if 'Rate Limit Exceeded' in str(resp_json):
                print("Rate Limited...Backing off for 2 minutes")
                time.sleep(60 * 2)
                retries += 1
                continue
            
            try:
                host = resp_json["baseUrl"]
                chapter_hash = resp_json["chapter"]["hash"]
                data = resp_json["chapter"]["data"]
                print("Received Response")
                break
            except Exception as e:
                retries = retries + 1
                print(f"Could not get host, hash or data: {e}, attempt {retries}")
                time.sleep(retries * 2)
                continue
        
        if retries > 10:
            print(f"Failed to fetch chapter data after {retries} attempts")
            return
            
        # Create table in main thread before starting worker threads
        self.db.create_page_urls_table(manga_id)
        
        threads = []
        for page in data:
            dict = {
                'hash': chapter_hash,
                'host': host,
                'page': page,
                'title': title,
                'chapter_num': chapter_num,
                'manga_id': manga_id
            }
            thread = threading.Thread(target=self.threaded_store_page_url, args=(dict, title, chapter_num, manga_id))
            threads.append(thread)
            thread.start()
    
        for thread in threads:
            thread.join()

    def threaded_store_page_url(self, dict, title, chapter_num, manga_id):
        thread_id = threading.get_ident()
        print(f"Thread ID: {thread_id}")
        
        # Construct the image URL
        image_url = f"{dict['host']}/data/{dict['hash']}/{dict['page']}"
        
        # Extract page number from page filename
        page_number = self.utils.get_first_number(dict['page'])
        
        # Store page URL to database
        try:
            self.db.store_page_url(
                manga_id=manga_id,
                manga_name=title,
                chapter_num=chapter_num,
                page_number=page_number,
                page_url=image_url
            )
            print(f"Stored page URL for {title} chapter {chapter_num} page {page_number}")
        except Exception as e:
            print(f"Failed to store page URL to database: {e}")
    
    def get_bucket_keys(self, base_key):
        keys = []
        print("Getting Keys")
        for obj in self.bucket.objects.filter(Prefix=f"{base_key}/"):
            obj = obj.key.rsplit('/')
            keys.append(self.utils.get_first_number(obj[2]))
        return keys
