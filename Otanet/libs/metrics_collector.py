import time
import threading
from collections import defaultdict, deque
from datetime import datetime
import json

class MetricsCollector:
    """
    Centralized metrics collection for the MangaDex scraper
    Thread-safe singleton that tracks all important metrics
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.persistence_file = "metrics_state.json"
        self._load_state()
        threading.Thread(target=self._auto_save_loop, daemon=True).start()

            
        self._initialized = True
        self.start_time = time.time()
        
        # Counters
        self.api_calls = {
            'manga_list': 0,
            'chapter_feed': 0,
            'page_urls': 0,
            'cover_art': 0,
            'total': 0
        }
        
        self.manga_stats = {
            'processed': 0,
            'new_manga': 0,
            'updated_manga': 0,
            'skipped_no_chapters': 0,
            'with_new_chapters': 0
        }
        
        self.chapter_stats = {
            'total_chapters': 0,
            'new_chapters': 0,
            'complete_chapters': 0,
            'partial_chapters': 0,
            'skipped_complete': 0
        }
        
        self.page_stats = {
            'total_pages': 0,
            'pages_downloaded': 0,
            'pages_skipped': 0,
            'failed_downloads': 0
        }
        
        self.s3_stats = {
            'uploads': 0,
            'upload_bytes': 0,
            'last_upload': None
        }
        
        self.error_stats = {
            'rate_limits': 0,
            'api_errors': 0,
            'db_errors': 0,
            'network_errors': 0
        }
        
        self.worker_stats = defaultdict(lambda: {
            'manga_processed': 0,
            'chapters_downloaded': 0,
            'current_manga': None,
            'last_activity': None
        })
        
        # Time series data (last 60 data points, 1 per second)
        self.api_rate = deque(maxlen=60)
        self.download_rate = deque(maxlen=60)
        
        # Thread for periodic calculations
        self.running = True
        self.metrics_thread = threading.Thread(target=self._calculate_rates, daemon=True)
        self.metrics_thread.start()
    
    def _calculate_rates(self):
        """Calculate per-second rates"""
        last_api_count = 0
        last_page_count = 0
        
        while self.running:
            current_api = self.api_calls['total']
            current_pages = self.page_stats['pages_downloaded']
            
            api_per_sec = current_api - last_api_count
            pages_per_sec = current_pages - last_page_count
            
            self.api_rate.append(api_per_sec)
            self.download_rate.append(pages_per_sec)
            
            last_api_count = current_api
            last_page_count = current_pages
            
            time.sleep(1)
    
    # API Call Tracking
    def record_api_call(self, call_type='other'):
        with self._lock:
            if call_type in self.api_calls:
                self.api_calls[call_type] += 1
            self.api_calls['total'] += 1
    
    # Manga Tracking
    def record_manga_processed(self, worker_id, manga_title, is_new=False, has_new_chapters=False):
        with self._lock:
            self.manga_stats['processed'] += 1
            if is_new:
                self.manga_stats['new_manga'] += 1
            else:
                self.manga_stats['updated_manga'] += 1
            
            if has_new_chapters:
                self.manga_stats['with_new_chapters'] += 1
            else:
                self.manga_stats['skipped_no_chapters'] += 1
            
            self.worker_stats[worker_id]['manga_processed'] += 1
            self.worker_stats[worker_id]['current_manga'] = manga_title
            self.worker_stats[worker_id]['last_activity'] = datetime.now()
    
    # Chapter Tracking
    def record_chapter(self, worker_id, is_new=False, is_complete=True, total_pages=0, downloaded_pages=0):
        with self._lock:
            self.chapter_stats['total_chapters'] += 1
            
            if is_new:
                self.chapter_stats['new_chapters'] += 1
            
            if is_complete:
                self.chapter_stats['complete_chapters'] += 1
            else:
                self.chapter_stats['partial_chapters'] += 1
            
            if downloaded_pages == 0:
                self.chapter_stats['skipped_complete'] += 1
            
            self.worker_stats[worker_id]['chapters_downloaded'] += 1
    
    # Page Tracking
    def record_pages(self, total, downloaded, skipped=0):
        with self._lock:
            self.page_stats['total_pages'] += total
            self.page_stats['pages_downloaded'] += downloaded
            self.page_stats['pages_skipped'] += skipped
    
    def record_page_failure(self):
        with self._lock:
            self.page_stats['failed_downloads'] += 1
    
    # S3 Tracking
    def record_s3_upload(self, bytes_uploaded=0):
        with self._lock:
            self.s3_stats['uploads'] += 1
            self.s3_stats['upload_bytes'] += bytes_uploaded
            self.s3_stats['last_upload'] = datetime.now()
    
    # Error Tracking
    def record_error(self, error_type):
        with self._lock:
            if error_type in self.error_stats:
                self.error_stats[error_type] += 1
    
    # Getters
    def get_uptime(self):
        return time.time() - self.start_time
    
    def get_current_api_rate(self):
        if len(self.api_rate) == 0:
            return 0
        return sum(self.api_rate) / len(self.api_rate)
    
    def get_current_download_rate(self):
        if len(self.download_rate) == 0:
            return 0
        return sum(self.download_rate) / len(self.download_rate)
    
    def get_all_metrics(self):
        """Return all metrics as a dictionary"""
        with self._lock:
            uptime = self.get_uptime()
            
            return {
                'uptime': uptime,
                'uptime_formatted': self._format_uptime(uptime),
                'api_calls': dict(self.api_calls),
                'manga_stats': dict(self.manga_stats),
                'chapter_stats': dict(self.chapter_stats),
                'page_stats': dict(self.page_stats),
                's3_stats': {
                    **dict(self.s3_stats),
                    'last_upload': self.s3_stats['last_upload'].isoformat() if self.s3_stats['last_upload'] else None
                },
                'error_stats': dict(self.error_stats),
                'rates': {
                    'api_per_second': self.get_current_api_rate(),
                    'pages_per_second': self.get_current_download_rate(),
                    'manga_per_hour': (self.manga_stats['processed'] / uptime * 3600) if uptime > 0 else 0,
                    'chapters_per_hour': (self.chapter_stats['total_chapters'] / uptime * 3600) if uptime > 0 else 0
                },
                'request_queue': active_requests,
                'worker_stats': {
                    str(worker_id): {
                            'manga_processed': stats['manga_processed'],
                            'chapters_downloaded': stats['chapters_downloaded'],
                            'current_manga': stats['current_manga'],
                            'last_activity': stats['last_activity'].isoformat() if stats['last_activity'] else None
                        }
                    for worker_id, stats in self.worker_stats.items()
                },
                'timestamp': datetime.now().isoformat()
            }
    
    def _format_uptime(self, seconds):
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m {secs}s"
        elif hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"
    
    def shutdown(self):
        self.running = False
        if self.metrics_thread.is_alive():
            self.metrics_thread.join(timeout=2)
