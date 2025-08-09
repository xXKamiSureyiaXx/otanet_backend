import requests
from manga_factory import MangaFactory

class MangaDexClient:
    def __init__(self):
        self.base_url = "https://api.mangadex.org"
        self.pagnation_limit = 5
        self.manga_dict = {}
        self.manga = []
    
    def set_manga_list(self):
        base_response = requests.get(
            f"{self.base_url}/manga",
            params={"limit": self.pagnation_limit}
        )

        for manga in base_response.json()["data"]:
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
            self.manga.append(MangaFactory(self.manga_dict))


            print(self.manga_dict)
            print()

        

        
        

