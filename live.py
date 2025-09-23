# live_location_server.py
from flask import Flask, request, render_template_string, jsonify
import csv
from datetime import datetime
import os

app = Flask(__name__)

CSV_FILE = "locations.csv"

# Ensure CSV file exists and has header
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_utc", "latitude", "longitude", "accuracy_m", "heading", "speed"])

HTML_PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Share Live Location</title>
</head>
<body>
  <h2>Live Location Sender</h2>
  <p id="status">Click "Start" to share location.</p>
  <button id="startBtn">Start</button>
  <button id="stopBtn" disabled>Stop</button>
  <pre id="log"></pre>

<script>
let watchId = null;
const statusEl = document.getElementById('status');
const logEl = document.getElementById('log');

function log(msg) {
  logEl.textContent = new Date().toISOString() + " - " + msg + "\\n" + logEl.textContent;
}

async function sendPosition(pos) {
  const data = {
    latitude: pos.coords.latitude,
    longitude: pos.coords.longitude,
    accuracy: pos.coords.accuracy || null,
    heading: pos.coords.heading || null,
    speed: pos.coords.speed || null,
    timestamp: pos.timestamp
  };
  try {
    const res = await fetch('/update', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
    });
    const txt = await res.text();
    log("Sent: " + JSON.stringify(data) + " -> Server: " + txt);
  } catch (err) {
    log("Network error sending position: " + err);
  }
}

document.getElementById('startBtn').addEventListener('click', () => {
  if (!("geolocation" in navigator)) {
    statusEl.textContent = "Geolocation not supported by this browser.";
    return;
  }
  statusEl.textContent = "Requesting permission...";
  watchId = navigator.geolocation.watchPosition(
    pos => {
      statusEl.textContent = "Sharing location (updates every time position changes).";
      sendPosition(pos);
    },
    err => {
      statusEl.textContent = "Geolocation error: " + err.message;
      log("Geolocation error: " + err.message);
    },
    {
      enableHighAccuracy: true,
      maximumAge: 1000,    // allow cached positions no older than 1s
      timeout: 10000
    }
  );
  document.getElementById('startBtn').disabled = true;
  document.getElementById('stopBtn').disabled = false;
});

document.getElementById('stopBtn').addEventListener('click', () => {
  if (watchId !== null) {
    navigator.geolocation.clearWatch(watchId);
    watchId = null;
    statusEl.textContent = "Stopped sharing location.";
    document.getElementById('startBtn').disabled = false;
    document.getElementById('stopBtn').disabled = true;
    log("Stopped watch.");
  }
});
</script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(HTML_PAGE)

@app.route("/update", methods=["POST"])
def update():
    data = request.get_json(force=True)
    # Expected fields: latitude, longitude, accuracy, heading, speed, timestamp
    try:
        lat = float(data.get("latitude"))
        lon = float(data.get("longitude"))
        accuracy = data.get("accuracy")
        heading = data.get("heading")
        speed = data.get("speed")
        timestamp_ms = data.get("timestamp")  # ms since epoch as provided by browser
        # Convert to ISO UTC
        if timestamp_ms:
            ts = datetime.utcfromtimestamp(float(timestamp_ms)/1000.0).isoformat() + "Z"
        else:
            ts = datetime.utcnow().isoformat() + "Z"

        # Append to CSV
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([ts, lat, lon, accuracy, heading, speed])

        print(f"[{ts}] {lat}, {lon} (acc={accuracy})")
        return jsonify({"status": "ok", "timestamp": ts})
    except Exception as e:
        print("Error processing update:", e)
        return jsonify({"status": "error", "error": str(e)}), 400

if __name__ == "__main__":
    # By default Flask runs on 127.0.0.1:5000 (localhost). To access from other devices on same Wi-Fi,
    # run with host='0.0.0.0' and open http://<your_machine_ip>:5000 on the phone.
    app.run(host="0.0.0.0", port=5000, debug=True)
