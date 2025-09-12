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
swap = False
index = 0
temp = 0
while True:
    try:
        manga_objs = []
        offset = index*25
        manga_list = mangadex_helper.get_recent_manga(offset)
        for manga in manga_list:
            os.chdir(root_dir)
            print("Creating Manga Obj")
            manga_obj = MangaFactory(manga)

            print("Setting Latest Chapter")
            should_download = mangadex_helper.set_latest_chapters(manga_obj)
            if should_download:
                print("Inserting into Database")
                sqlite_helper.insert_manga_metadata("manga_metadata", manga_obj)

                print("Downloading Chapters")
                mangadex_helper.download_chapters(manga_obj)
            time.sleep(1*60)
        
        if swap:
            index = temp
            swap = False
        index = index + 1

        if index > 6:
            index = 0
            temp = 0

        if index > 3:
            temp = index
            index = 0
            swap = True

        print('Sleeping 10 minute')
        time.sleep(10*60)
    except Exception as e:
        if '429' in str(e) or '403' in str(e):
            print(f"Failed with: {e}")
            print('Sleeping 10 mins')
            time.sleep(60*10)
        else:
            print(f"Sleeping for 5 minutes: {e}")
            time.sleep(5*10)
            continue


        