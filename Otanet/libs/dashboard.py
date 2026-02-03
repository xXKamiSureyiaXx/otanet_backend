from flask import Flask, render_template, jsonify, request
from metrics_collector import MetricsCollector
from mangadex_helper import MangaDexHelper
from manga_factory import MangaFactory
import threading
from queue import Queue
import time
helper = MangaDexHelper()

app = Flask(__name__)
metrics = MetricsCollector()

request_queue = Queue()
active_requests = {}  # manga_id -> status


@app.route('/')
def dashboard():
    """Render the main dashboard"""
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    data = metrics.get_all_metrics()
    data['request_queue'] = dict(active_requests)  # ensure serializable copy
    return jsonify(data)


@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'uptime': metrics.get_uptime()})


@app.route('/api/request-manga', methods=['POST'])
def request_manga():
    data = request.get_json()
    manga_id = data.get("manga_id")

    if not manga_id:
        return jsonify({"error": "manga_id required"}), 400

    if manga_id in active_requests:
        return jsonify({"status": "already queued"})

    active_requests[manga_id] = "queued"
    request_queue.put(manga_id)

    return jsonify({"status": "queued"})



def request_queue_worker():
    helper = MangaDexHelper()

    while True:
        manga_id = request_queue.get()
        active_requests[manga_id] = "processing"

        try:
            print(f"[QUEUE] Processing requested manga {manga_id}")
            manga_data = helper.get_requested_manga(manga_id)

            if not manga_data:
                active_requests[manga_id] = "failed"
                continue

            manga = MangaFactory(manga_data)
            should_download = helper.set_latest_chapters(manga)

            if should_download:
                helper.download_chapters(manga)
                helper.data_to_s3()

            active_requests[manga_id] = "completed"
            print(f"[QUEUE] Finished {manga_id}")

        except Exception as e:
            print(f"[QUEUE ERROR] {manga_id}: {e}")
            active_requests[manga_id] = "failed"

        time.sleep(1)


def process_requested_manga(manga_id):
    print(f"Operator requested manga {manga_id}")
    
    manga_data = helper.get_requested_manga(manga_id)
    if not manga_data:
        print(f"Could not find manga {manga_id}")
        return

    manga = MangaFactory(manga_data)

    should_download = helper.set_latest_chapters(manga)
    if should_download:
        helper.download_chapters(manga)
        helper.data_to_s3()
        print(f"Finished processing requested manga {manga_id}")
    else:
        print(f"No new chapters for requested manga {manga_id}")


def run_dashboard(host='0.0.0.0', port=5000):
    """Run the Flask dashboard in a separate thread"""
    dashboard_thread = threading.Thread(
        target=lambda: app.run(host=host, port=port, debug=False, use_reloader=False),
        daemon=True
    )
    threading.Thread(target=request_queue_worker, daemon=True).start()
    dashboard_thread.start()
    print(f"Dashboard started at http://{host}:{port}")
    return dashboard_thread

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
