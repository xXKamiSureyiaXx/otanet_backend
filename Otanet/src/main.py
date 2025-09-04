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


while True:
    try:
        manga_objs = []
        offset = 0*100
        manga_list = client.get_recent_manga(offset)
        for manga in manga_list:
            manga_obj = MangaFactory(manga)
            client.download_chapters(manga_obj)
            sqlite_helper.insert_manga_metadata("manga_metadata", manga_obj)
            time.sleep(10*60)
    except:
        time.sleep(60*60)