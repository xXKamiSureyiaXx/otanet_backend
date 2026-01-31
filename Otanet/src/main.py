import os
import sys
import time
from queue import Queue
from threading import Thread
import random
path = os.getcwd()
parent_dir = os.path.abspath(os.path.join(path, os.pardir))
###### Import Player Object ######
sys.path.insert(0, f'{parent_dir}//libs')
from mangadex_helper import MangaDexHelper
from manga_factory import MangaFactory
from sqlite_helper import SQLiteHelper
##################################

def worker_thread(worker_id, offset_queue, root_dir):
    """Each worker continuously processes manga from offset queue"""
    # Each worker gets its own helper instances to avoid contention
    mangadex_helper = MangaDexHelper()
    sqlite_helper = SQLiteHelper()
    
    while True:
        try:
            offset = offset_queue.get()
            time.sleep(random.uniform(1, 8.0))  # Stagger requests slightly
            if offset is None:  # Poison pill to stop thread
                offset_queue.task_done()
                break
                
            print(f"[Worker {worker_id}] Processing offset {offset}")
            
            manga_list = mangadex_helper.get_recent_manga(offset)
            
            for manga in manga_list:
                print(f"[Worker {worker_id}] Creating Manga Obj")
                manga_obj = MangaFactory(manga)
                
                print(f"[Worker {worker_id}] Setting Latest Chapter")
                should_download = mangadex_helper.set_latest_chapters(manga_obj)
                
                if should_download:
                    print(f"[Worker {worker_id}] Inserting into Database")
                    sqlite_helper.insert_manga_metadata("manga_metadata", manga_obj)
                    
                    print(f"[Worker {worker_id}] Downloading chapters for {manga_obj.get_title()}")
                    mangadex_helper.download_chapters(manga_obj)
                sqlite_helper.data_to_s3()
                    
                print(f"[Worker {worker_id}] Processed (or skipped); sleeping briefly")
                
            
            offset_queue.task_done()
            
        except Exception as e:
            if '429' in str(e) or '403' in str(e):
                print(f"[Worker {worker_id}] Rate limited: {e}")
                time.sleep(60)  # Back off for a minute
            else:
                print(f"[Worker {worker_id}] Error: {e}")
                time.sleep(5)
            offset_queue.task_done()

# Main execution
root_dir = os.getcwd()

# Initialize database table with a single instance
sqlite_helper = SQLiteHelper()
sqlite_helper.create_metadata_table('manga_metadata')

# Create queue for work distribution
offset_queue = Queue()

# Start 10 worker threads
NUM_WORKERS = 10
workers = []
for i in range(NUM_WORKERS):
    t = Thread(target=worker_thread, args=(i, offset_queue, root_dir))
    t.daemon = True
    t.start()
    time.sleep(3) # Stagger thread starts
    workers.append(t)

print(f"Started {NUM_WORKERS} worker threads")

# Main loop: continuously add offsets to the queue
index = 0
temp = 0
swap = False

while True:
    try:
        # Add next batch of offsets to queue (10 offsets for 10 workers)
        for i in range(NUM_WORKERS):
            offset = (index + i) * 10
            offset_queue.put(offset)
            print(f"[Main] Queued offset {offset}")
        
        # Update index using your existing logic
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
        
        print('[Main] Sleeping 10 minutes')
        time.sleep(10*60)
        
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")
        # Send poison pills to stop all workers
        for _ in range(NUM_WORKERS):
            offset_queue.put(None)
        # Wait for workers to finish
        for t in workers:
            t.join()
        print("[Main] All workers stopped")
        break
    except Exception as e:
        print(f"[Main] Error: {e}")
        time.sleep(5*60)