import os
import sys
import time
path = os.getcwd()
parent_dir = os.path.abspath(os.path.join(path, os.pardir))

###### Import Player Object ######
sys.path.insert(0, f'{parent_dir}//libs')
from client_factory import MangaDexClient
from manga_factory import MangaFactory
from sqlite_helper import SQLiteHelper
##################################


client = MangaDexClient()
sqlite_helper = SQLiteHelper()

root_dir = os.getcwd()
while True:
    try:
        manga_objs = []
        offset = 0*100
        manga_list = client.get_recent_manga(offset)
        for manga in manga_list:
            os.chdir(root_dir)
            manga_obj = MangaFactory(manga)
            should_download = client.set_latest_chapters(manga_obj)
            if should_download:
                sqlite_helper.insert_manga_metadata("manga_metadata", manga_obj)
                client.download_chapters(manga_obj) 
            time.sleep(60)  
        time.sleep(5*60)
    except Exception as e:
        print(f"Failed with: {e}")
        time.sleep(60*60)