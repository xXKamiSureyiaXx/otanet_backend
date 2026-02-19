import os
import sys
import time
import platform
import threading
from queue import Queue
from threading import Thread, Lock
import random

path = os.getcwd()
parent_dir = os.path.abspath(os.path.join(path, os.pardir))

###### Import Libraries ######
sys.path.insert(0, f'{parent_dir}//libs')
from mangadex_helper import MangaDexHelper
from natomanga_helper import NatoMangaHelper
from manga_factory import MangaFactory
from sqlite_helper import SQLiteHelper
from metrics_collector import MetricsCollector
from dashboard import run_dashboard
##################################

# ─────────────────────────────────────────────────────────────────────────────
# Browser setup
# pyvirtualdisplay is Linux-only. On Windows the browser window is minimized.
# undetected-chromedriver is used for all NatoManga requests.
# A single driver + lock is shared across the pipeline — selenium is not
# thread-safe so only one NatoManga worker thread is used.
# ─────────────────────────────────────────────────────────────────────────────

def init_browser():
    """
    Start pyvirtualdisplay (Linux) or minimized Chrome (Windows/Mac),
    then return (driver, driver_lock).
    """
    import undetected_chromedriver as uc

    IS_LINUX = platform.system() == "Linux"

    if IS_LINUX:
        try:
            from pyvirtualdisplay import Display
            display = Display(visible=0, size=(1920, 1080))
            display.start()
            print("[Browser] Virtual display started (Linux)")
        except Exception as exc:
            print(f"[Browser] pyvirtualdisplay failed ({exc}), continuing without it")

    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    if platform.system() == "Linux":
        options.binary_location = "/usr/bin/google-chrome"

    driver = uc.Chrome(options=options, headless=False)


    if not IS_LINUX:
        driver.minimize_window()
        print("[Browser] Chrome started (minimized)")

    # Warm up — let Cloudflare set its clearance cookies on the homepage
    print("[Browser] Warming up on NatoManga homepage...")
    driver.get("https://www.natomanga.com")
    time.sleep(5)

    deadline = time.time() + 30
    while "Just a moment" in driver.title:
        if time.time() > deadline:
            print("[Browser] WARNING: Still on CF challenge page after warmup")
            break
        print("[Browser] Waiting for CF challenge to clear...")
        time.sleep(3)

    print(f"[Browser] Warmup complete – page title: {driver.title}")

    driver_lock = Lock()
    return driver, driver_lock


# ─────────────────────────────────────────────────────────────────────────────
# Global tracking for concurrent manga processing
# ─────────────────────────────────────────────────────────────────────────────

processing_manga = set()
processing_lock  = Lock()


# ─────────────────────────────────────────────────────────────────────────────
# MangaDex worker  (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

def mangadex_worker(worker_id, offset_queue, s3_upload_queue):
    helper        = MangaDexHelper()
    sqlite_helper = SQLiteHelper()
    metrics       = MetricsCollector()

    print(f"[Worker MD-{worker_id}] Started")

    while True:
        try:
            offset = offset_queue.get()
            time.sleep(random.uniform(1, 8.0))

            if offset is None:
                offset_queue.task_done()
                break

            print(f"[Worker MD-{worker_id}] Processing offset {offset}")

            try:
                manga_list = helper.get_recent_manga(offset)
                metrics.record_api_call("manga_list")
            except Exception as exc:
                metrics.record_error("api_errors")
                print(f"[Worker MD-{worker_id}] Error fetching list: {exc}")
                offset_queue.task_done()
                continue

            for manga in manga_list:
                try:
                    manga_obj = MangaFactory(manga)
                    manga_id  = manga_obj.get_id()

                    with processing_lock:
                        if manga_id in processing_manga:
                            continue
                        processing_manga.add(manga_id)

                    try:
                        existing_chapter = sqlite_helper.get_manga_latest_chapter(
                            "manga_metadata", manga_obj.get_id()
                        )
                        is_new_manga     = existing_chapter is None
                        should_download  = helper.set_latest_chapters(manga_obj)

                        metrics.record_manga_processed(
                            worker_id=worker_id,
                            manga_title=manga_obj.get_title(),
                            is_new=is_new_manga,
                            has_new_chapters=should_download,
                        )

                        if should_download:
                            sqlite_helper.insert_manga_metadata("manga_metadata", manga_obj)
                            helper.download_chapters(manga_obj)
                            s3_upload_queue.put(True)
                        else:
                            print(f"[Worker MD-{worker_id}] No new chapters for "
                                  f"'{manga_obj.get_title()}'")
                    finally:
                        with processing_lock:
                            processing_manga.discard(manga_id)

                except Exception as exc:
                    metrics.record_error("api_errors")
                    print(f"[Worker MD-{worker_id}] Error: {exc}")

            offset_queue.task_done()

        except Exception as exc:
            if "429" in str(exc) or "403" in str(exc):
                print(f"[Worker MD-{worker_id}] Rate limited – sleeping 60s")
                metrics.record_error("rate_limits")
                time.sleep(60)
            else:
                print(f"[Worker MD-{worker_id}] Error: {exc}")
                metrics.record_error("api_errors")
                time.sleep(5)
            offset_queue.task_done()


# ─────────────────────────────────────────────────────────────────────────────
# NatoManga worker  (single thread — shares one browser instance)
# ─────────────────────────────────────────────────────────────────────────────

def natomanga_worker(offset_queue, s3_upload_queue, driver, driver_lock):
    """
    Single-threaded worker for NatoManga.
    All browser navigation is serialized through driver_lock inside
    NatoMangaHelper._get_html(), so this thread is the only consumer.
    """
    helper        = NatoMangaHelper(driver, driver_lock)
    sqlite_helper = SQLiteHelper()
    metrics       = MetricsCollector()

    print("[Worker NM] Started")

    while True:
        try:
            offset = offset_queue.get()
            time.sleep(random.uniform(2, 8.0))

            if offset is None:
                offset_queue.task_done()
                break

            print(f"[Worker NM] Processing offset {offset}")

            try:
                manga_list = helper.get_recent_manga(offset)
                metrics.record_api_call("manga_list")
            except Exception as exc:
                metrics.record_error("api_errors")
                print(f"[Worker NM] Error fetching list: {exc}")
                offset_queue.task_done()
                continue

            for manga in manga_list:
                try:
                    manga_obj = MangaFactory(manga)
                    manga_id  = manga_obj.get_id()

                    with processing_lock:
                        if manga_id in processing_manga:
                            continue
                        processing_manga.add(manga_id)

                    try:
                        existing_chapter = sqlite_helper.get_manga_latest_chapter(
                            "manga_metadata", manga_obj.get_id()
                        )
                        is_new_manga    = existing_chapter is None
                        should_download = helper.set_latest_chapters(manga_obj)

                        metrics.record_manga_processed(
                            worker_id="NM",
                            manga_title=manga_obj.get_title(),
                            is_new=is_new_manga,
                            has_new_chapters=should_download,
                        )

                        if should_download:
                            sqlite_helper.insert_manga_metadata("manga_metadata", manga_obj)
                            helper.download_chapters(manga_obj)
                            s3_upload_queue.put(True)
                        else:
                            print(f"[Worker NM] No new chapters for '{manga_obj.get_title()}'")
                    finally:
                        with processing_lock:
                            processing_manga.discard(manga_id)

                except Exception as exc:
                    metrics.record_error("api_errors")
                    print(f"[Worker NM] Error: {exc}")

            offset_queue.task_done()

        except Exception as exc:
            print(f"[Worker NM] Error: {exc}")
            metrics.record_error("api_errors")
            time.sleep(5)
            offset_queue.task_done()


# ─────────────────────────────────────────────────────────────────────────────
# S3 upload thread  (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

def s3_upload_thread(s3_upload_queue):
    sqlite_helper   = SQLiteHelper()
    metrics         = MetricsCollector()
    upload_count    = 0
    last_upload_time = time.time()

    while True:
        try:
            try:
                s3_upload_queue.get(timeout=60)
                upload_count += 1
            except Exception:
                pass

            current_time = time.time()
            time_elapsed = current_time - last_upload_time

            if upload_count >= 10 or time_elapsed >= 300:
                print(f"[S3 Upload] Uploading DB "
                      f"(count={upload_count}, elapsed={time_elapsed:.0f}s)")
                try:
                    sqlite_helper.data_to_s3()
                    metrics.record_s3_upload()
                    upload_count     = 0
                    last_upload_time = current_time
                except Exception as exc:
                    print(f"[S3 Upload] Error: {exc}")
                    metrics.record_error("api_errors")

        except Exception as exc:
            print(f"[S3 Upload] Error: {exc}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

root_dir = os.getcwd()

print("Initialising metrics collector...")
metrics = MetricsCollector()

print("Starting dashboard server...")
run_dashboard(host="0.0.0.0", port=5000)
print("Dashboard available at http://localhost:5000")

sqlite_helper = SQLiteHelper()
sqlite_helper.create_metadata_table("manga_metadata")

# ── Browser init (must happen before NatoManga worker starts) ─────────────────
print("Initialising browser for NatoManga...")
nato_driver, nato_driver_lock = init_browser()

# ── Queues ────────────────────────────────────────────────────────────────────
mangadex_queue  = Queue()
natomanga_queue = Queue()
s3_upload_queue = Queue()

# ── S3 upload thread ──────────────────────────────────────────────────────────
s3_thread = Thread(target=s3_upload_thread, args=(s3_upload_queue,))
s3_thread.daemon = True
s3_thread.start()
print("Started S3 upload thread")

# ── MangaDex workers (4 threads) ──────────────────────────────────────────────
MANGADEX_WORKERS = 4
mangadex_threads = []
for i in range(MANGADEX_WORKERS):
    t = Thread(target=mangadex_worker,
               args=(i, mangadex_queue, s3_upload_queue))
    t.daemon = True
    t.start()
    mangadex_threads.append(t)
print(f"Started {MANGADEX_WORKERS} MangaDex worker threads")

# ── NatoManga worker (1 thread — single browser) ─────────────────────────────
NATOMANGA_WORKERS = 3  # each gets its own Chrome

nato_browsers = []
for i in range(NATOMANGA_WORKERS):
    print(f"[Browser] Starting browser {i + 1}/{NATOMANGA_WORKERS}...")
    driver, lock = init_browser()
    nato_browsers.append((driver, lock))
    time.sleep(3)  # stagger startup

nato_threads = []
for i, (driver, lock) in enumerate(nato_browsers):
    t = Thread(target=natomanga_worker,
               args=(natomanga_queue, s3_upload_queue, driver, lock))
    t.daemon = True
    t.start()
    nato_threads.append(t)
print(f"Started {NATOMANGA_WORKERS} NatoManga worker threads")

# ── Configuration ─────────────────────────────────────────────────────────────
MANGADEX_MAX_OFFSET  = 4000
NATOMANGA_MAX_OFFSET = 24 * 100   # 50 pages
CYCLE_SLEEP          = 5 * 60

print("\n" + "=" * 60)
print("Multi-Source Manga Scraper Started!")
print("=" * 60)
print(f"Dashboard        : http://localhost:5000")
print(f"MangaDex workers : {MANGADEX_WORKERS}  (offsets 0-{MANGADEX_MAX_OFFSET})")
print(f"NatoManga workers: 1  (offsets 0-{NATOMANGA_MAX_OFFSET}, browser-driven)")
print("=" * 60 + "\n")

md_cycle_offset = 0
nm_cycle_offset = 0

while True:
    try:
        # MangaDex offsets
        for i in range(MANGADEX_WORKERS):
            offset = 0 if i == 0 else (
                ((md_cycle_offset + i - 1) % (MANGADEX_MAX_OFFSET // 8)) * 8 + 8
            )
            mangadex_queue.put(offset)
            print(f"[Main] MangaDex offset queued: {offset}")
        md_cycle_offset += MANGADEX_WORKERS - 1

        # NatoManga offset (one page per cycle — browser is slow)
        nm_offset = ((nm_cycle_offset) % (NATOMANGA_MAX_OFFSET // 24)) * 24
        natomanga_queue.put(nm_offset)
        print(f"[Main] NatoManga offset queued: {nm_offset}")
        nm_cycle_offset += 1

        print(f"[Main] Sleeping {CYCLE_SLEEP // 60} minutes")
        time.sleep(CYCLE_SLEEP)

    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")

        print("[Main] Performing final S3 upload...")
        sqlite_helper.data_to_s3()

        # Poison pills
        for _ in range(MANGADEX_WORKERS):
            mangadex_queue.put(None)
        natomanga_queue.put(None)

        for t in mangadex_threads + nato_threads:
            t.join()

        print("[Main] Closing browser...")

        for driver, _ in nato_browsers:
            try:
                driver.quit()
            except Exception:
                pass

        metrics.shutdown()
        print("[Main] Shutdown complete")
        break

    except Exception as exc:
        print(f"[Main] Error: {exc}")
        metrics.record_error("api_errors")
        time.sleep(CYCLE_SLEEP)