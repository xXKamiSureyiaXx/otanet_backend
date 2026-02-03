import os
import sys
import time
from queue import Queue
from threading import Thread
import random

path = os.getcwd()
parent_dir = os.path.abspath(os.path.join(path, os.pardir))

###### Import Libraries ######
sys.path.insert(0, f'{parent_dir}//libs')
from mangadex_helper import MangaDexHelper
from manga_factory import MangaFactory
from sqlite_helper import SQLiteHelper
from metrics_collector import MetricsCollector
from dashboard import run_dashboard
##################################

def worker_thread(worker_id, offset_queue, root_dir, s3_upload_queue):
    """Each worker continuously processes manga from offset queue"""
    # Each worker gets its own helper instances to avoid contention
    mangadex_helper = MangaDexHelper()
    sqlite_helper = SQLiteHelper()
    metrics = MetricsCollector()
    
    while True:
        try:
            offset = offset_queue.get()
            time.sleep(random.uniform(1, 8.0))  # Stagger requests slightly
            
            if offset is None:  # Poison pill to stop thread
                offset_queue.task_done()
                break
                
            print(f"[Worker {worker_id}] Processing offset {offset}")
            
            try:
                manga_list = mangadex_helper.get_recent_manga(offset)
                metrics.record_api_call('manga_list')
            except Exception as e:
                metrics.record_error('api_errors')
                print(f"[Worker {worker_id}] Error fetching manga list: {e}")
                offset_queue.task_done()
                continue
            
            for manga in manga_list:
                try:
                    print(f"[Worker {worker_id}] Creating Manga Obj for {manga['title']}")
                    manga_obj = MangaFactory(manga)
                    
                    # Check if this is new manga
                    existing_chapter = sqlite_helper.get_manga_latest_chapter('manga_metadata', manga_obj.get_id())
                    is_new_manga = existing_chapter is None
                    
                    print(f"[Worker {worker_id}] Setting Latest Chapter")
                    should_download = mangadex_helper.set_latest_chapters(manga_obj)
                    
                    # Record manga processing
                    metrics.record_manga_processed(
                        worker_id=worker_id,
                        manga_title=manga_obj.get_title(),
                        is_new=is_new_manga,
                        has_new_chapters=should_download
                    )
                    
                    if should_download:
                        print(f"[Worker {worker_id}] Inserting into Database")
                        sqlite_helper.insert_manga_metadata("manga_metadata", manga_obj)
                        
                        print(f"[Worker {worker_id}] Downloading chapters for {manga_obj.get_title()}")
                        mangadex_helper.download_chapters(manga_obj)
                        
                        # Signal that S3 upload should happen
                        s3_upload_queue.put(True)
                    else:
                        print(f"[Worker {worker_id}] No new chapters for {manga_obj.get_title()}, skipping download")
                        
                    print(f"[Worker {worker_id}] Processed (or skipped); sleeping briefly")
                    
                except Exception as e:
                    metrics.record_error('api_errors')
                    print(f"[Worker {worker_id}] Error processing manga: {e}")
                    continue
            
            offset_queue.task_done()
            
        except Exception as e:
            if '429' in str(e) or '403' in str(e):
                print(f"[Worker {worker_id}] Rate limited: {e}")
                metrics.record_error('rate_limits')
                time.sleep(60)  # Back off for a minute
            else:
                print(f"[Worker {worker_id}] Error: {e}")
                metrics.record_error('api_errors')
                time.sleep(5)
            offset_queue.task_done()

def s3_upload_thread(s3_upload_queue):
    """
    OPTIMIZATION: Batch S3 uploads instead of uploading after every manga
    Upload every 5 minutes or after 10 manga processed
    """
    sqlite_helper = SQLiteHelper()
    metrics = MetricsCollector()
    upload_count = 0
    last_upload_time = time.time()
    
    while True:
        try:
            # Wait for signal with timeout
            try:
                s3_upload_queue.get(timeout=60)
                upload_count += 1
            except:
                pass  # Timeout, check if we should upload anyway
            
            current_time = time.time()
            time_elapsed = current_time - last_upload_time
            
            # Upload if: 10 manga processed OR 5 minutes passed
            if upload_count >= 10 or time_elapsed >= 300:
                print(f"[S3 Upload] Uploading database (count: {upload_count}, time elapsed: {time_elapsed:.0f}s)")
                try:
                    sqlite_helper.data_to_s3()
                    metrics.record_s3_upload()
                    upload_count = 0
                    last_upload_time = current_time
                except Exception as e:
                    print(f"[S3 Upload] Error: {e}")
                    metrics.record_error('api_errors')
                
        except Exception as e:
            print(f"[S3 Upload] Error: {e}")
            time.sleep(5)

# Main execution
root_dir = os.getcwd()

# Initialize metrics and dashboard
print("Initializing metrics collector...")
metrics = MetricsCollector()

print("Starting dashboard server...")
run_dashboard(host='0.0.0.0', port=5000)
print("Dashboard available at http://localhost:5000")

# Initialize database table with a single instance
sqlite_helper = SQLiteHelper()
sqlite_helper.create_metadata_table('manga_metadata')

# Create queues for work distribution and S3 upload signaling
offset_queue = Queue()
s3_upload_queue = Queue()

# Start S3 upload thread
s3_thread = Thread(target=s3_upload_thread, args=(s3_upload_queue,))
s3_thread.daemon = True
s3_thread.start()
print("Started S3 upload thread")

# Start 10 worker threads
NUM_WORKERS = 10
workers = []
for i in range(NUM_WORKERS):
    t = Thread(target=worker_thread, args=(i, offset_queue, root_dir, s3_upload_queue))
    t.daemon = True
    t.start()
    workers.append(t)
print(f"Started {NUM_WORKERS} worker threads")

# Main loop: continuously add offsets to the queue
cycle_offset = 0
MAX_OFFSET = 3000  # Cycle back after reaching this offset

print("\n" + "="*60)
print("MangaDex Scraper Started Successfully!")
print("="*60)
print(f"Dashboard: http://localhost:5000")
print(f"Workers: {NUM_WORKERS}")
print(f"Offset Range: 0-{MAX_OFFSET}")
print("="*60 + "\n")

while True:
    try:
        # Add next batch of offsets to queue (10 offsets for 10 workers)
        for i in range(NUM_WORKERS):
            if i == 0:
                offset = 0  # Worker 0 always gets offset 0
            else:
                offset = ((cycle_offset + i - 1) % (MAX_OFFSET // 10)) * 10 + 10
            offset_queue.put(offset)
            print(f"[Main] Queued offset {offset}")
        
        # Move to next cycle (only affects workers 1-9)
        cycle_offset += (NUM_WORKERS - 1)
        
        print('[Main] Sleeping 5 minutes')
        time.sleep(5*60)
        
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")
        
        # Final S3 upload before shutdown
        print("[Main] Performing final S3 upload...")
        sqlite_helper.data_to_s3()
        
        # Send poison pills to stop all workers
        for _ in range(NUM_WORKERS):
            offset_queue.put(None)
        
        # Wait for workers to finish
        for t in workers:
            t.join()
        
        # Shutdown metrics
        metrics.shutdown()
        
        print("[Main] All workers stopped")
        print("[Main] Shutdown complete")
        break
        
    except Exception as e:
        print(f"[Main] Error: {e}")
        metrics.record_error('api_errors')
        time.sleep(5*60)
