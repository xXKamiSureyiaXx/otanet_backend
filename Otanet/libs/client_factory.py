import requests
import os
import re
import boto3


class MangaDexClient:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = 'otanet-manga-devo'
        self.base_url = "https://api.mangadex.org"
        self.pagnation_limit = 5
        self.manga_dict = {}
        self.manga_list = []
    
    def get_manga_list(self):
        base_response = requests.get(
            f"{self.base_url}/manga",
            params={"limit": self.pagnation_limit}
        )

        for manga in base_response.json()["data"]:
            try:
                self.manga_dict = {}
                self.manga_dict["id"] = manga["id"]
                self.manga_dict["title"] = manga["attributes"]["title"]["en"]
                self.manga_dict["description"] = manga["attributes"]["description"]["en"]

                tags = []
                for tag in manga["attributes"]["tags"]:
                    tags.append(tag["attributes"]["name"]["en"])
                self.manga_dict["tags"] = tags

                manga_relationships = next((obj for obj in manga["relationships"] if obj["type"] == "cover_art"), False)
                cover_response = requests.get(f"{self.base_url}/cover/{manga_relationships['id']}")
                cover_id = cover_response.json()["data"]["attributes"]["fileName"]

                self.manga_dict["cover_img"] = f"https://uploads.mangadex.org/covers/{manga['id']}/{cover_id}"
                self.manga_list.append(self.manga_dict)
            except:
                continue
        return self.manga_list
    
    def download_chapters(self, title, cover_img, manga_id):
        def is_float(s):
            try:
                float(s)
                return True
            except:
                return False
            
        languages = ["en"]
        chapters = requests.get(
            f"{self.base_url}/manga/{manga_id}/feed",
            params={"translatedLanguage[]": languages},
        )
        chapters = list(filter(lambda chapter_num: is_float(chapter_num['attributes']['chapter']) != False, chapters.json()["data"]))
        chapters = sorted(chapters, key=lambda chapter_num: float(chapter_num['attributes']['chapter']))

        temp_index = 0
        for chapter in chapters:
            # Temporarily limiting each manga to 3 chapters for the sake of development
            if temp_index >= 3:
                break
            temp_index = temp_index + 1

            chapter_id = chapter["id"]
            chapter_num = chapter["attributes"]["chapter"]
            chapter_resp = requests.get(f"{self.base_url}/at-home/server/{chapter_id}")
            resp_json = chapter_resp.json()

            host = resp_json["baseUrl"]
            chapter_hash = resp_json["chapter"]["hash"]
            data = resp_json["chapter"]["data"]

            # Making a folder to store the images in. Titles sometimes have 
            # symbols so those will be removed when creating directories
            cleaned_title = re.sub(r'[^a-zA-Z0-9]', '', title)
            os.chdir("/tmp/")
            folder_path = f"{cleaned_title}/chapter_{chapter_num}"
            os.makedirs(folder_path, exist_ok=True)

            
            for page in data:
                print(f"Downloading {chapter_hash}")
                r = requests.get(f"{host}/data/{chapter_hash}/{page}")
                img_data = requests.get(cover_img).content
                with open(f"{folder_path}/{page}", mode="wb") as f:
                    f.write(r.content)
                with open(f"{folder_path}/title", mode="wb") as f:
                    f.write(img_data)
                try:
                    s3_obj_title_key = f"{cleaned_title}/0_title/cover_img"
                    s3_obj_key = f"{cleaned_title}/chapter_{chapter_num}/{page}"
                    self.s3_client.upload_file(f"{folder_path}/{page}", self.bucket_name, s3_obj_key, ExtraArgs={'ContentType': "image/png"})
                    self.s3_client.upload_file(f"{folder_path}/title", self.bucket_name, s3_obj_title_key, ExtraArgs={'ContentType': "image/png"})
                    os.remove(f"{folder_path}/{page}")
                except Exception as e:
                    print(f"Error writing PNG to S3: {e}")