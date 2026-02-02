from flask import Flask, render_template, jsonify
from metrics_collector import MetricsCollector
import threading

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
