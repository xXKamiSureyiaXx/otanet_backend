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
        self.client.download_chapters(self.title, self.cover_img, self.id)
    
    def get_id(self):
        return self.id
    
    def get_title(self):
        return self.title
    
    def get_description(self):
        return self.description
    
    def get_tags(self):
        return self.tags
    
