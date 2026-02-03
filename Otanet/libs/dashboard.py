from flask import Flask, render_template, jsonify, request
from metrics_collector import MetricsCollector
from mangadex_helper import MangaDexHelper
from manga_factory import MangaFactory
import threading
helper = MangaDexHelper()

app = Flask(__name__)
metrics = MetricsCollector()

@app.route('/')
def dashboard():
    """Render the main dashboard"""
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    """API endpoint to get current metrics"""
    return jsonify(metrics.get_all_metrics())

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

    thread = threading.Thread(target=process_requested_manga, args=(manga_id,), daemon=True)
    thread.start()

    return jsonify({"status": "started", "manga_id": manga_id})

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
    dashboard_thread.start()
    print(f"Dashboard started at http://{host}:{port}")
    return dashboard_thread

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
