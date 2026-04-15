import io
import os
import json
import csv
import secrets as _secrets
import requests
import threading
from functools import wraps
from flask import Flask, request, jsonify, Response
from playwright.sync_api import sync_playwright

# ========================
# CONFIG
# ========================
SCRAPEGRAPH_API_KEY = os.environ.get("SCRAPEGRAPH_API_KEY", "")
if not SCRAPEGRAPH_API_KEY:
    print("[WARNING] SCRAPEGRAPH_API_KEY not set. ScrapeGraphAI calls will fail.")

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", _secrets.token_hex(32))

# Optional API key authentication — set SCRAPER_API_KEY env var to enable
_SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")

def require_api_key(f):
    """Decorator that enforces API key auth when SCRAPER_API_KEY is set."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if _SCRAPER_API_KEY:
            provided = request.headers.get("X-API-Key", "")
            if provided != _SCRAPER_API_KEY:
                return jsonify({"error": "Unauthorized – invalid or missing X-API-Key header"}), 401
        return f(*args, **kwargs)
    return decorated

scraping_results = []
scraping_running = False

# ========================
# ScrapeGraphAI API Call
# ========================
def scrape_with_scrapegraph(query: str):
    url = "https://api.scrapegraphai.com/v1/scrape"
    headers = {
        "Authorization": f"Bearer {SCRAPEGRAPH_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "query": f"Find detailed business info for {query} from Google Maps including "
                 f"name, phone number, website, email, social media links, rating, reviews, "
                 f"pricing, and open/close status."
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        return response.json().get("data", {})
    except Exception as e:
        print(f"[ScrapeGraphAI Error] {e}")
        return {}

# ========================
# Playwright Fallback
# ========================
def scrape_with_playwright(query: str):
    data = {}
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(f"https://www.google.com/maps/search/{query}", timeout=60000)

            page.wait_for_timeout(5000)

            data["name"] = page.query_selector("h1") and page.query_selector("h1").inner_text()
            data["phone"] = page.query_selector("button[aria-label*='Phone']") and page.query_selector("button[aria-label*='Phone']").inner_text()
            data["website"] = page.query_selector("a[aria-label*='Website']") and page.query_selector("a[aria-label*='Website']").get_attribute("href")
            data["status"] = page.query_selector("div[aria-label*='Closed']") and page.query_selector("div[aria-label*='Closed']").inner_text()
            data["rating"] = page.query_selector("span[aria-label*='stars']") and page.query_selector("span[aria-label*='stars']").inner_text()
            data["reviews"] = page.query_selector("span[aria-label*='reviews']") and page.query_selector("span[aria-label*='reviews']").inner_text()

            browser.close()
    except Exception as e:
        print(f"[Playwright Error] {e}")

    return data

# ========================
# Worker Thread
# ========================
def scraper_worker(queries):
    global scraping_results, scraping_running
    scraping_results = []
    scraping_running = True

    for q in queries:
        print(f"🔎 Scraping: {q}")
        result = scrape_with_scrapegraph(q)

        # fallback to Playwright if ScrapeGraph misses fields
        if not result.get("phone") or not result.get("status"):
            fallback = scrape_with_playwright(q)
            result.update({k: v for k, v in fallback.items() if v})

        scraping_results.append(result)

    scraping_running = False

# ========================
# Flask Routes
# ========================
@app.route("/")
def home():
    return jsonify({
        "message": "Google Maps Scraper API (Hybrid: ScrapeGraphAI + Playwright)",
        "routes": {
            "/start": "POST - body: {\"queries\": [\"Starbucks New York\"]}",
            "/status": "GET - check if scraping is running",
            "/get-results": "GET - fetch results as JSON or CSV (?format=csv)"
        }
    })

@app.route("/start", methods=["POST"])
@require_api_key
def start_scraping():
    data = request.json or {}
    queries = data.get("queries", [])
    if not queries or not isinstance(queries, list):
        return jsonify({"error": "No queries provided"}), 400
    if len(queries) > 100:
        return jsonify({"error": "Too many queries (max 100)"}), 400

    threading.Thread(target=scraper_worker, args=(queries,)).start()
    return jsonify({"message": "Scraping started", "query_count": len(queries)})

@app.route("/status", methods=["GET"])
def status():
    return jsonify({"running": scraping_running, "completed": len(scraping_results)})

@app.route("/get-results", methods=["GET"])
@require_api_key
def get_results():
    fmt = request.args.get("format", "json")
    if fmt == "csv":
        def generate():
            output = io.StringIO()
            fieldnames = list(scraping_results[0].keys()) if scraping_results else []
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for row in scraping_results:
                writer.writerow(row)
            yield output.getvalue()
        return Response(generate(), mimetype="text/csv")
    return jsonify(scraping_results)

# ========================
# MAIN
# ========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
