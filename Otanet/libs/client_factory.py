import requests
import os
import re
import boto3
import botocore.exceptions
import string
import time


class MangaDexClient:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = 'otanet-manga-devo'
        self.base_url = "https://api.mangadex.org"
        self.pagnation_limit = 25
        self.manga_dict = {}
        self.manga_list = []
        self.languages = ["en"]
    
    def get_recent_manga(self, offset):
        base_response = requests.get(
            f"{self.base_url}/manga",
            params={"limit": self.pagnation_limit,
                    "offset": offset}
        )

        for manga in base_response.json()["data"]:
            try:
                pattern = r'[^a-zA-Z0-9\s' + re.escape(string.punctuation) + ']'
                self.manga_dict = {}
                self.manga_dict["id"] = manga["id"]
                self.manga_dict["title"] = re.sub(pattern, '', manga["attributes"]["title"]["en"])
                self.manga_dict["description"] = re.sub(pattern, '', manga["attributes"]["description"]["en"])

                tags = []
                for tag in manga["attributes"]["tags"]:
                    tags.append(re.sub(pattern, '', tag["attributes"]["name"]["en"]))
                self.manga_dict["tags"] = tags

                manga_relationships = next((obj for obj in manga["relationships"] if obj["type"] == "cover_art"), False)
                cover_response = requests.get(f"{self.base_url}/cover/{manga_relationships['id']}")
                cover_id = cover_response.json()["data"]["attributes"]["fileName"]

                self.manga_dict["cover_img"] = f"https://uploads.mangadex.org/covers/{manga['id']}/{cover_id}"
                self.manga_list.append(self.manga_dict)
            except:
                continue
        return self.manga_list
    def set_latest_chapters(self, manga):
        def is_float(s):
            try:
                float(s)
                return True
            except:
                return False
        chapters = requests.get(
            f"{self.base_url}/manga/{manga.get_id()}/feed",
            params={"translatedLanguage[]": self.languages},
        )
        chapters = list(filter(lambda chapter_num: is_float(chapter_num['attributes']['chapter']) != False, chapters.json()["data"]))
        chapters = sorted(chapters, key=lambda chapter_num: float(chapter_num['attributes']['chapter']))
        try:
            latest_chapter = float(chapters[-1]['attributes']['chapter'])
            manga.set_latest_chapter(latest_chapter)
            return True
        except:
            print(f"Could not latest chapter for manga {manga.get_id()}")
            return False
        
    def download_chapters(self, manga):
        def is_float(s):
            try:
                float(s)
                return True
            except:
                return False
            
        chapters = requests.get(
            f"{self.base_url}/manga/{manga.get_id()}/feed",
            params={"translatedLanguage[]": self.languages},
        )
        chapters = list(filter(lambda chapter_num: is_float(chapter_num['attributes']['chapter']) != False, chapters.json()["data"]))
        chapters = sorted(chapters, key=lambda chapter_num: float(chapter_num['attributes']['chapter']))
        try:
            latest_chapter = float(chapters[-1]['attributes']['chapter'])
            manga.set_latest_chapter(latest_chapter)
        except Exception as e:
            print(f"Could not latest chapter for manga {manga.get_id()}")
            return 1
        limit = 0
        sleep = 0
        for chapter in chapters:
            if limit > 25:
                break
            chapter_id = chapter["id"]
            chapter_num = chapter["attributes"]["chapter"].replace('.', '_')
            chapter_resp = requests.get(f"{self.base_url}/at-home/server/{chapter_id}")
            resp_json = chapter_resp.json()

            host = resp_json["baseUrl"]
            chapter_hash = resp_json["chapter"]["hash"]
            data = resp_json["chapter"]["data"]

            # Making a folder to store the images in. Titles sometimes have 
            # symbols so those will be removed when creating directories
            cleaned_title = manga.get_title().lower().strip()
            cleaned_title = re.sub(r"[^a-z0-9 ]", "", cleaned_title)
            cleaned_title = re.sub(r"\s+", "-", cleaned_title)
            home_dir = os.getcwd()
            os.makedirs('tmp', exist_ok=True) 
            os.chdir("/tmp/")
            folder_path = f"{manga.get_id()}/chapter_{chapter_num}"
            os.makedirs(folder_path, exist_ok=True)
            time.sleep(sleep)

            base_key = f"{cleaned_title}/chapter_{chapter_num}" 
            s3_resource = boto3.resource('s3')
            bucket = s3_resource.Bucket('otanet-manga-devo')
            keys = []

            img_data = requests.get(manga.get_cover_img()).content
            s3_obj_title_key = f"{cleaned_title}/0_title/cover_img"
            try:
                self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_obj_title_key)
            except:
                with open(f"{folder_path}/title", mode="wb") as f:
                    f.write(img_data)
                    self.s3_client.upload_file(f"{folder_path}/title", self.bucket_name, s3_obj_title_key, ExtraArgs={'ContentType': "image/png"})
                os.remove(f"{folder_path}/title")

            for obj in bucket.objects.filter(Prefix=f"{base_key}/"):
                keys.append(obj.key)
            
            downloaded = False
            for page in data:
                s3_obj_key = f"{cleaned_title}/chapter_{chapter_num}/{page}"
                if s3_obj_key in keys:
                    sleep = 1
                    continue
                print(f"Request for {manga.get_id()}")
                r = requests.get(f"{host}/data/{chapter_hash}/{page}")
                
                # Check if chapter exists and if it doesn't download it to S3
                print(f"Downloading {page}")
                with open(f"{folder_path}/{page}", mode="wb") as f:
                    sleep = 15
                    downloaded = True
                    f.write(r.content)
                self.s3_client.upload_file(f"{folder_path}/{page}", self.bucket_name, s3_obj_key, ExtraArgs={'ContentType': "image/png"})
                os.remove(f"{folder_path}/{page}")
            os.chdir(home_dir)
            if downloaded:
                limit = limit + 1
            self.data_to_s3()
    def data_to_s3(self):
        print("Updating Database")
        self.s3_client.upload_file(f"otanet_devo.db", self.bucket_name, "database/otanet_devo.db")

    def get_requested_manga(self, manga_id):

        manga = requests.get(
            f"{self.base_url}/manga/{manga_id}",
                params={"translatedLanguage[]": self.languages},
            ).json()
        
        try:
            pattern = r'[^a-zA-Z0-9\s' + re.escape(string.punctuation) + ']'
            self.manga_dict = {}
            self.manga_dict["id"] = manga["data"]["id"]
            self.manga_dict["title"] = re.sub(pattern, '', manga["data"]["attributes"]["title"]["en"])
            self.manga_dict["description"] = re.sub(pattern, '', manga["data"]["attributes"]["description"]["en"])

            tags = []
            for tag in manga["data"]["attributes"]["tags"]:
                tags.append(re.sub(pattern, '', tag["attributes"]["name"]["en"]))
            self.manga_dict["tags"] = tags

            manga_relationships = next((obj for obj in manga["data"]["relationships"] if obj["type"] == "cover_art"), False)
            cover_response = requests.get(f"{self.base_url}/cover/{manga_relationships['id']}")
            cover_id = cover_response.json()["data"]["attributes"]["fileName"]

            self.manga_dict["cover_img"] = f"https://uploads.mangadex.org/covers/{manga['data']['id']}/{cover_id}"
            return self.manga_dict
        except:
            raise "Manga Not Found"
        


