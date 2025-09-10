from mangadex_helper import MangaDexHelper
import pandas as pd
import os
from utils import Utils

class MangaFactory:
    def __init__(self, params):
        self.id = params["id"]
        self.title = params["title"]
        self.description = params["description"]
        self.tags = params["tags"]
        self.cover_img = params["cover_img"]
        self.latest_chapter = 0 
        self.chapters = []
        self.utils = Utils()
        self.csv_name = 'manga_metadata.csv'
    
    def get_id(self):
        return self.id
    
    def get_title(self):
        return self.title
    
    def get_description(self):
        return self.description
    
    def get_tags(self):
        return self.tags
    
    def get_cover_img(self):
        return self.cover_img
    
    def get_latest_chapter(self):
        return self.latest_chapter
    
    def get_chapters(self):
        return self.chapters
    
    def set_latest_chapter(self, chapters):
        try:
            self.latest_chapter = float(chapters[-1]['attributes']['chapter'])
        except:
            print(f"Could not set latest chapter for manga {self.id}")

    def set_chapters(self, chapters):
        chapters = list(filter(lambda chapter_num: self.utils.is_float(chapter_num['attributes']['chapter']) != False, chapters.json()["data"]))
        chapters = sorted(chapters, key=lambda chapter_num: float(chapter_num['attributes']['chapter']))
        self.chapters = chapters
    
    def should_append(self):
        append = True
        existing_data = pd.read_csv(self.csv_name)
        if self.id in existing_data['id'].values:
            append = False
        return append
    
    def init_csv(self):
        if not os.path.exists(self.csv_name):
            init_csv = pd.DataFrame(columns=['id', 'title', 'description', 'tags'])
            init_csv.to_csv(self.csv_name)

    #### DEPRECATED ####
    def store_data(self):
        self.init_csv()
        manga_metadata = pd.DataFrame(
            [{'id':self.id, 'title':self.title, 'description':self.description, 'tags':self.tags}], 
            columns=['id', 'title', 'description', 'tags'])
        
        if self.should_append():
            with open(self.csv_name, 'a') as f:
                print("Appending Data")
                manga_metadata.to_csv(f, header=False)
        else:
            print("Data exists in dataframe")