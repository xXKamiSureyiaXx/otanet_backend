import requests
import os
import re
import boto3
import botocore.exceptions
import string
import time
import threading
from utils import Utils


class MangaDexHelper:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.s3_resource.Bucket('otanet-manga-devo')
        self.utils = Utils()
        self.bucket_name = 'otanet-manga-devo'
        self.base_url = "https://api.mangadex.org"
        self.pagnation_limit = 100
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
        chapters = requests.get(
            f"{self.base_url}/manga/{manga.get_id()}/feed",
            params={"translatedLanguage[]": self.languages},
        )
        manga.set_chapters(chapters) 
        should_download = manga.set_latest_chapter()
        return should_download
        
    def download_chapters(self, manga):  
        limit = 0
        for chapter in manga.get_chapters():
            print(f"Request for {manga.get_id()}")
            print(f"Starting download for index {limit}")
            if limit > 50:
                break

            # Making a folder to store the images in. Titles sometimes have 
            # symbols so those will be removed when creating directories
            title = self.utils.normalize_s3_text(manga.get_title())
            chapter_num = chapter["attributes"]["chapter"].replace('.', '_')
            chapter_path = f"{manga.get_id()}/chapter_{chapter_num}"
            base_key = f"{title}/chapter_{chapter_num}"

            if chapter != manga.get_chapters()[-1]:
                s3_dir = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=f"{base_key}/", MaxKeys=1)
                if s3_dir['KeyCount'] > 0:       
                    print(f"Chapter Exists...Skipping: {base_key}")
                    continue
                else:
                    print("Chapter does not Exist...Continuing")

            
            self.utils.create_tmp_dir(chapter_path)
            print("Dowloading Cover")
            self.download_cover(chapter_path, title, manga.get_cover_img())

            print("Downloading Chapters")
            did_download = self.download_pages(chapter_path, chapter['id'], title, chapter_num, self.get_bucket_keys(base_key))
            
            os.chdir(self.root_directory)
            self.data_to_s3()

            if did_download:
                limit = limit + 1
                print(limit)
            time.sleep(2)            

    def data_to_s3(self):
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
                    'title': self.utils(manga["data"]["attributes"]["title"]["en"]),
                    'description': self.utils(manga["data"]["attributes"]["description"]["en"]),
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
    
    def download_pages(self, chapter_path, chapter_id, title, chapter_num, keys):
        print("Starting Download for Pages")
        downloaded = False
        chapter_resp = requests.get(f"{self.base_url}/at-home/server/{chapter_id}")
        resp_json = chapter_resp.json()
  
        try:
            host = resp_json["baseUrl"]
            chapter_hash = resp_json["chapter"]["hash"]
            data = resp_json["chapter"]["data"]
        except Exception as e:
            print(f"Could not host, hash or data: {e}")
        print("Recieved Response")

        threads = []
        for page in data:
            print("Processing Data")
            s3_obj_key = f"{title}/chapter_{chapter_num}/{page}"
            if self.utils.get_first_number(page) in keys:
                print(f"Skipping page {self.utils.get_first_number(page)}")
                downloaded = False
                continue
            downloaded = True

            dict = {
                'hash': chapter_hash,
                'host': host,
                'key': s3_obj_key,
                'page': page
            }
            path = f"{chapter_path}/page"
            print(path)
            print("Starting Threads")
            self.threaded_download(dict, path)
            #thread = threading.Thread(target=self.threaded_download, args=(dict,path,))
            #threads.append(thread)
            #thread.start()
            #time.sleep(1)

        for thread in threads:
            thread.join()

        return downloaded
    
    def threaded_download(self, page, path):
        content = requests.get(f"{page['host']}/data/{page['hash']}/{page['page']}")
        tries = 0
        while tries < 20:
            try:
                with open(path, mode="wb") as f:
                    f.write(content.content)
                    self.s3_client.upload_file(path, self.bucket_name, page['key'], ExtraArgs={'ContentType': "image/png"})
                    break
            except Exception as e:
                print(f"Failed to upload: {e}, attempt {tries}")
                tries = tries + 1
                time.sleep(tries)
                continue
            try:
                os.remove(path)
                break
            except Exception as e:
                print(f"Failed to remove {path}: {e}")
                tries = tries + 1
                continue
            
    
    def get_bucket_keys(self, base_key):
        keys = []
        print("Getting Keys")
        for obj in self.bucket.objects.filter(Prefix=f"{base_key}/"):
            obj = obj.key.rsplit('/')
            keys.append(self.utils.get_first_number(obj[2]))
        return keys