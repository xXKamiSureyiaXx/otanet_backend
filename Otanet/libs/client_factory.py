import requests
import os
import re


class MangaDexClient:
    def __init__(self):
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
    
    def download_chapters(self, title, manga_id):
        languages = ["en"]

        chapters = requests.get(
            f"{self.base_url}/manga/{manga_id}/feed",
            params={"translatedLanguage[]": languages},
        )

        temp_index = 0
        for chapter in chapters.json()["data"]:
            # Temporarily limiting each manga to 3 chapters for the sake of development
            if temp_index >= 3:
                break
            temp_index = temp_index + 1

            chapter_id = chapter["id"]
            chapter_resp = requests.get(f"{self.base_url}/at-home/server/{chapter_id}")
            resp_json = chapter_resp.json()

            host = resp_json["baseUrl"]
            chapter_hash = resp_json["chapter"]["hash"]
            data = resp_json["chapter"]["data"]

            # Making a folder to store the images in. Titles sometimes have 
            # symbols so those will be removed when creating directories
            cleaned_title = re.sub(r'[^a-zA-Z0-9]', '', title)
            folder_path = f"Mangadex/{cleaned_title}/{chapter_id}"
            os.makedirs(folder_path, exist_ok=True)

            
            for page in data:
                print(f"Downloading {chapter_hash}")
                r = requests.get(f"{host}/data/{chapter_hash}/{page}")

                with open(f"{folder_path}/{page}", mode="wb") as f:
                    f.write(r.content)