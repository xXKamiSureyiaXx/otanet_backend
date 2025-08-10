from client_factory import MangaDexClient

class MangaFactory:
    def __init__(self, params):
        self.id = params["id"]
        self.title = params["title"]
        self.description = params["description"]
        self.tags = params["tags"]
        self.cover_img = params["cover_img"]
        self.client = MangaDexClient()

    def download_manga(self):
        self.client.download_chapters(self.title, self.id)