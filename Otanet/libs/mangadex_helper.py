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
from metrics_collector import MetricsCollector
import random


class MangaDexHelper:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.s3_resource.Bucket('otanet-manga-devo')
        self.utils = Utils()
        self.db = SQLiteHelper()
        self.metrics = MetricsCollector()
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
        
        self.metrics.record_api_call('manga_list')

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
        existing_chapter = self.db.get_manga_latest_chapter(manga.get_id().replace('-','_'), manga.get_id())
        
        all_chapters = []
        offset = 0
        
        # Fetch all chapters (we need to know the absolute latest)
        while True:
            time.sleep(random.uniform(5, 20.0))  # Stagger requests
            response = requests.get(
                f"{self.base_url}/manga/{manga.get_id()}/feed",
                params={"translatedLanguage[]": self.languages, "offset": offset, "limit": 100},
            )
            
            self.metrics.record_api_call('chapter_feed')

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
        IMPROVED: Check which pages are missing from each chapter
        Download only missing pages instead of skipping entire chapters
        """
        # Get existing chapter pages from database
        existing_chapters_status = self.db.get_chapters_with_status(manga.get_id())
        print(f"Found {len(existing_chapters_status)} existing chapters in database for manga {manga.get_id()}")
        
        worker_id = threading.current_thread().name.split('-')[0] if '-' in threading.current_thread().name else 0
        
        for chapter in manga.get_chapters():
            chapter_num = chapter["attributes"]["chapter"].replace('.', '_')
            
            # Check if this chapter has any existing pages
            existing_pages = set()
            is_new_chapter = chapter_num not in existing_chapters_status
            
            if chapter_num in existing_chapters_status:
                existing_pages = existing_chapters_status[chapter_num]['pages']
                print(f"Chapter {chapter_num} has {len(existing_pages)} existing pages in database")
            
            time.sleep(random.uniform(5, 20.0))  # Stagger chapter downloads
            print(f"Request for {manga.get_id()} chapter {chapter_num}")

            title = self.utils.normalize_s3_text(manga.get_title())
            chapter_path = f"{manga.get_id()}/chapter_{chapter_num}"
            base_key = f"{title}/chapter_{chapter_num}"
            
            print("Storing Page URLs to Database (only missing pages)")
            # Pass existing pages so we only store new ones
            pages_info = self.store_page_url_to_database(
                chapter['id'], 
                title, 
                chapter_num, 
                manga.get_id(), 
                existing_pages
            )
            
            # Record chapter metrics
            if pages_info:
                is_complete = pages_info['downloaded'] == 0  # Complete if no pages downloaded
                self.metrics.record_chapter(
                    worker_id=worker_id,
                    is_new=is_new_chapter,
                    is_complete=is_complete,
                    total_pages=pages_info['total'],
                    downloaded_pages=pages_info['downloaded']
                )
                
                self.metrics.record_pages(
                    total=pages_info['total'],
                    downloaded=pages_info['downloaded'],
                    skipped=pages_info['skipped']
                )
            
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
        
        self.metrics.record_api_call('manga_list')
        
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
            self.metrics.record_error('api_errors')
        
    def get_manga_cover_id(self, manga_relationships):
        cover_response = requests.get(f"{self.base_url}/cover/{manga_relationships['id']}")
        self.metrics.record_api_call('cover_art')
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
    
    def store_page_url_to_database(self, chapter_id, title, chapter_num, manga_id, existing_pages=None):
        """
        IMPROVED: Only fetch and store pages that aren't already in the database
        
        Args:
            chapter_id: MangaDex chapter ID
            title: Manga title
            chapter_num: Chapter number
            manga_id: Manga ID
            existing_pages: Set of page numbers already in database (optional)
            
        Returns:
            dict with 'total', 'downloaded', 'skipped' page counts
        """
        if existing_pages is None:
            existing_pages = set()
        
        print(f"Fetching page URLs from MangaDex API for chapter {chapter_num}...")

        retries = 0
        host = None
        chapter_hash = None
        data = None
        
        while retries <= 10:
            try:
                chapter_resp = requests.get(f"{self.base_url}/at-home/server/{chapter_id}")
                resp_json = chapter_resp.json()
                
                self.metrics.record_api_call('page_urls')

                if 'Rate Limit Exceeded' in str(resp_json):
                    print("Rate Limited...Backing off for 2 minutes")
                    self.metrics.record_error('rate_limits')
                    time.sleep(60 * 2)
                    retries += 1
                    continue
                
                host = resp_json["baseUrl"]
                chapter_hash = resp_json["chapter"]["hash"]
                data = resp_json["chapter"]["data"]
                print("Received Response")
                break
            except Exception as e:
                retries = retries + 1
                print(f"Could not get host, hash or data: {e}, attempt {retries}")
                self.metrics.record_error('api_errors')
                time.sleep(retries * 2)
                continue
        
        if retries > 10 or data is None:
            print(f"Failed to fetch chapter data after {retries} attempts")
            return None
        
        # Count total pages and missing pages
        total_pages = len(data)
        
        # Determine which pages are missing
        missing_pages = []
        for page in data:
            page_number = str(self.utils.get_first_number(page))
            if page_number not in existing_pages:
                missing_pages.append(page)
        
        if len(missing_pages) == 0:
            print(f"Chapter {chapter_num} is complete with all {total_pages} pages, skipping...")
            return {
                'total': total_pages,
                'downloaded': 0,
                'skipped': total_pages
            }
        
        print(f"Chapter {chapter_num}: {len(existing_pages)}/{total_pages} pages exist, downloading {len(missing_pages)} missing pages")
            
        # Create table in main thread before starting worker threads
        self.db.create_page_urls_table(manga_id)
        
        # Only process missing pages
        threads = []
        for page in missing_pages:
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
        
        print(f"Completed storing {len(missing_pages)} missing pages for chapter {chapter_num}")
        
        return {
            'total': total_pages,
            'downloaded': len(missing_pages),
            'skipped': len(existing_pages)
        }

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
            self.metrics.record_page_failure()
            self.metrics.record_error('db_errors')
    
    def get_bucket_keys(self, base_key):
        keys = []
        print("Getting Keys")
        for obj in self.bucket.objects.filter(Prefix=f"{base_key}/"):
            obj = obj.key.rsplit('/')
            keys.append(self.utils.get_first_number(obj[2]))
        return keys
