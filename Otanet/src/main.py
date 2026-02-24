import os
import sys
import time
import threading
from queue import Queue
from threading import Thread, Lock
import random

path = os.getcwd()
parent_dir = os.path.abspath(os.path.join(path, os.pardir))

###### Import Libraries ######
sys.path.insert(0, f'{parent_dir}//libs')
from mangadex_helper import MangaDexHelper
from asura_helper import AsuraComicHelper
from manga_factory import MangaFactory
from sqlite_helper import SQLiteHelper
from metrics_collector import MetricsCollector
from dashboard import run_dashboard
##################################

# ─────────────────────────────────────────────────────────────────────────────
# Global tracking for concurrent manga processing
# ─────────────────────────────────────────────────────────────────────────────

processing_manga = set()
processing_lock  = Lock()


# ─────────────────────────────────────────────────────────────────────────────
# Generic worker factory
# Both MangaDex and AsuraComic workers follow the exact same logic; the only
# difference is the helper instance that is passed in.
# ─────────────────────────────────────────────────────────────────────────────

def _process_manga_list(manga_list, helper, sqlite_helper, metrics, worker_label, s3_upload_queue):
    """Shared inner loop: process a list of manga dicts from any source."""
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
                    worker_id=worker_label,
                    manga_title=manga_obj.get_title(),
                    is_new=is_new_manga,
                    has_new_chapters=should_download,
                )

                if should_download:
                    sqlite_helper.insert_manga_metadata("manga_metadata", manga_obj)
                    helper.download_chapters(manga_obj)
                    s3_upload_queue.put(True)
                else:
                    print(f"[{worker_label}] No new chapters for '{manga_obj.get_title()}'")
            finally:
                with processing_lock:
                    processing_manga.discard(manga_id)

        except Exception as exc:
            metrics.record_error("api_errors")
            print(f"[{worker_label}] Error processing manga: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# MangaDex worker
# ─────────────────────────────────────────────────────────────────────────────

def mangadex_worker(worker_id, offset_queue, s3_upload_queue):
    helper        = MangaDexHelper()
    sqlite_helper = SQLiteHelper()
    metrics       = MetricsCollector()
    label         = f"Worker MD-{worker_id}"

    print(f"[{label}] Started")

    while True:
        try:
            offset = offset_queue.get()
            time.sleep(random.uniform(1, 8.0))

            if offset is None:
                offset_queue.task_done()
                break

            print(f"[{label}] Processing offset {offset}")

            try:
                manga_list = helper.get_recent_manga(offset)
                metrics.record_api_call("manga_list")
            except Exception as exc:
                metrics.record_error("api_errors")
                print(f"[{label}] Error fetching list: {exc}")
                offset_queue.task_done()
                continue

            _process_manga_list(manga_list, helper, sqlite_helper, metrics, label, s3_upload_queue)
            offset_queue.task_done()

        except Exception as exc:
            if "429" in str(exc) or "403" in str(exc):
                print(f"[{label}] Rate limited – sleeping 60s")
                metrics.record_error("rate_limits")
                time.sleep(60)
            else:
                print(f"[{label}] Error: {exc}")
                metrics.record_error("api_errors")
                time.sleep(5)
            offset_queue.task_done()


# ─────────────────────────────────────────────────────────────────────────────
# AsuraComic worker
# Uses plain requests – no browser, no lock, safe to run multiple threads.
# ─────────────────────────────────────────────────────────────────────────────

def asura_worker(worker_id, offset_queue, s3_upload_queue):
    helper        = AsuraComicHelper()
    sqlite_helper = SQLiteHelper()
    metrics       = MetricsCollector()
    label         = f"Worker AS-{worker_id}"

    print(f"[{label}] Started")

    while True:
        try:
            offset = offset_queue.get()
            time.sleep(random.uniform(1, 5.0))

            if offset is None:
                offset_queue.task_done()
                break

            print(f"[{label}] Processing offset {offset}")

            try:
                manga_list = helper.get_recent_manga(offset)
                metrics.record_api_call("manga_list")
            except Exception as exc:
                metrics.record_error("api_errors")
                print(f"[{label}] Error fetching list: {exc}")
                offset_queue.task_done()
                continue

            _process_manga_list(manga_list, helper, sqlite_helper, metrics, label, s3_upload_queue)
            offset_queue.task_done()

        except Exception as exc:
            if "429" in str(exc):
                print(f"[{label}] Rate limited – sleeping 60s")
                metrics.record_error("rate_limits")
                time.sleep(60)
            else:
                print(f"[{label}] Error: {exc}")
                metrics.record_error("api_errors")
                time.sleep(5)
            offset_queue.task_done()


# ─────────────────────────────────────────────────────────────────────────────
# S3 upload thread
# ─────────────────────────────────────────────────────────────────────────────

def s3_upload_thread(s3_upload_queue):
    sqlite_helper    = SQLiteHelper()
    metrics          = MetricsCollector()
    upload_count     = 0
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

# ── Queues ────────────────────────────────────────────────────────────────────
mangadex_queue  = Queue()
asura_queue     = Queue()
s3_upload_queue = Queue()

# ── S3 upload thread ──────────────────────────────────────────────────────────
s3_thread = Thread(target=s3_upload_thread, args=(s3_upload_queue,))
s3_thread.daemon = True
s3_thread.start()
print("Started S3 upload thread")

# ── MangaDex workers ──────────────────────────────────────────────────────────
MANGADEX_WORKERS = 4
mangadex_threads = []
for i in range(MANGADEX_WORKERS):
    t = Thread(target=mangadex_worker, args=(i, mangadex_queue, s3_upload_queue))
    t.daemon = True
    t.start()
    mangadex_threads.append(t)
print(f"Started {MANGADEX_WORKERS} MangaDex worker threads")

# ── AsuraComic workers (requests-based, safe to multithread) ──────────────────
ASURA_WORKERS = 2
asura_threads = []
for i in range(ASURA_WORKERS):
    t = Thread(target=asura_worker, args=(i, asura_queue, s3_upload_queue))
    t.daemon = True
    t.start()
    asura_threads.append(t)
print(f"Started {ASURA_WORKERS} AsuraComic worker threads")

# ── Configuration ─────────────────────────────────────────────────────────────
MANGADEX_MAX_OFFSET = 4000
ASURA_MAX_OFFSET    = 20 * 100   # 100 pages × 20 items
CYCLE_SLEEP         = 5 * 60

print("\n" + "=" * 60)
print("Multi-Source Manga Scraper Started!")
print("=" * 60)
print(f"Dashboard         : http://localhost:5000")
print(f"MangaDex workers  : {MANGADEX_WORKERS}  (offsets 0–{MANGADEX_MAX_OFFSET})")
print(f"AsuraComic workers: {ASURA_WORKERS}  (offsets 0–{ASURA_MAX_OFFSET})")
print("=" * 60 + "\n")

md_cycle_offset    = 0
asura_cycle_offset = 0

while True:
    try:
        # ── MangaDex offsets ──────────────────────────────────────────────────
        for i in range(MANGADEX_WORKERS):
            offset = 0 if i == 0 else (
                ((md_cycle_offset + i - 1) % (MANGADEX_MAX_OFFSET // 8)) * 8 + 8
            )
            mangadex_queue.put(offset)
            print(f"[Main] MangaDex offset queued: {offset}")
        md_cycle_offset += MANGADEX_WORKERS - 1

        # ── AsuraComic offsets (one per worker per cycle) ─────────────────────
        for i in range(ASURA_WORKERS):
            asura_offset = (
                ((asura_cycle_offset + i) % (ASURA_MAX_OFFSET // ASURA_WORKERS))
                * ASURA_WORKERS * 20
            )
            asura_queue.put(asura_offset)
            print(f"[Main] AsuraComic offset queued: {asura_offset}")
        asura_cycle_offset += 1

        print(f"[Main] Sleeping {CYCLE_SLEEP // 60} minutes")
        time.sleep(CYCLE_SLEEP)

    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")

        print("[Main] Performing final S3 upload...")
        sqlite_helper.data_to_s3()

        # Poison pills
        for _ in range(MANGADEX_WORKERS):
            mangadex_queue.put(None)
        for _ in range(ASURA_WORKERS):
            asura_queue.put(None)

        for t in mangadex_threads + asura_threads:
            t.join()

        metrics.shutdown()
        print("[Main] Shutdown complete")
        break

    except Exception as exc:
        print(f"[Main] Error: {exc}")
        metrics.record_error("api_errors")
        time.sleep(CYCLE_SLEEP)