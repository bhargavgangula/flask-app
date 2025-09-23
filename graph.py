import os
import json
import csv
import requests
import threading
from flask import Flask, request, jsonify, Response
from playwright.sync_api import sync_playwright

# ========================
# CONFIG
# ========================
SCRAPEGRAPH_API_KEY = "sgai-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  # replace with your key

app = Flask(__name__)
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
def start_scraping():
    data = request.json
    queries = data.get("queries", [])
    if not queries:
        return jsonify({"error": "No queries provided"}), 400

    threading.Thread(target=scraper_worker, args=(queries,)).start()
    return jsonify({"message": "Scraping started", "queries": queries})

@app.route("/status", methods=["GET"])
def status():
    return jsonify({"running": scraping_running, "completed": len(scraping_results)})

@app.route("/get-results", methods=["GET"])
def get_results():
    fmt = request.args.get("format", "json")
    if fmt == "csv":
        def generate():
            fieldnames = list(scraping_results[0].keys()) if scraping_results else []
            writer = csv.DictWriter(Response.stream, fieldnames=fieldnames)
            writer.writeheader()
            for row in scraping_results:
                writer.writerow(row)
            yield ""
        return Response(generate(), mimetype="text/csv")
    return jsonify(scraping_results)

# ========================
# MAIN
# ========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
