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
from concurrent.futures import ThreadPoolExecutor, as_completed
##################################


mangadex_helper = MangaDexHelper()
sqlite_helper = SQLiteHelper()
sqlite_helper.create_metadata_table('manga_metadata')

root_dir = os.getcwd()
swap = False
index = 0
temp = 0
while True:
    try:
        manga_objs = []
        offset = index*25
        manga_list = mangadex_helper.get_recent_manga(offset)
        # Collect manga objects that need downloading, insert metadata synchronously
        to_download = []
        for manga in manga_list:
            os.chdir(root_dir)
            print("Creating Manga Obj")
            manga_obj = MangaFactory(manga)

            print("Setting Latest Chapter")
            should_download = mangadex_helper.set_latest_chapters(manga_obj)
            if should_download:
                print("Inserting into Database")
                sqlite_helper.insert_manga_metadata("manga_metadata", manga_obj)
                to_download.append(manga_obj)
            print("Queued (or skipped); sleeping briefly to avoid rate limits")
            time.sleep(1)

        # Run downloads with up to 10 concurrent threads
        if to_download:
            print(f"Starting threaded downloads for {len(to_download)} manga(s) with up to 10 workers")
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_manga = {executor.submit(mangadex_helper.download_chapters, m): m for m in to_download}
                for future in as_completed(future_to_manga):
                    m = future_to_manga[future]
                    try:
                        result = future.result()
                        print(f"Finished download task for {m.get_title()}")
                    except Exception as e:
                        print(f"Download task raised for {m.get_title()}: {e}")
        
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
        index = index + 1
        if '429' in str(e) or '403' in str(e):
            print(f"Failed with: {e}")
            print('Sleeping 10 mins')
            time.sleep(60*10)
        else:
            print(f"Sleeping for 5 minutes: {e}")
            time.sleep(5*10)
            continue


        