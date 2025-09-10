import os
import sys
import time
path = os.getcwd()
parent_dir = os.path.abspath(os.path.join(path, os.pardir))

###### Import Player Object ######
sys.path.insert(0, f'{parent_dir}//libs')
from mangadex_helper import MangaDexHelper
from manga_factory import MangaFactory
from sqlite_helper import SQLiteHelper
##################################


mangadex_helper = MangaDexHelper()
sqlite_helper = SQLiteHelper()

root_dir = os.getcwd()
while True:
    try:
        manga_objs = []
        offset = 0*100
        manga_list = mangadex_helper.get_recent_manga(offset)
        for manga in manga_list:
            os.chdir(root_dir)
            manga_obj = MangaFactory(manga)
            should_download = mangadex_helper.set_latest_chapters(manga_obj)
            if should_download:
                sqlite_helper.insert_manga_metadata("manga_metadata", manga_obj)
                mangadex_helper.download_chapters(manga_obj) 
            time.sleep(10)  
        time.sleep(1*60)
    except Exception as e:
        print(f"Failed with: {e}")
        print('Sleeping 10 mins')
        time.sleep(60*10)