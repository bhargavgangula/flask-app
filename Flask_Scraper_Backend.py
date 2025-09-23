#!/usr/bin/env python3
"""
index.py - Enhanced end-to-end Google Maps scraper + Flask UI.
Features:
 - ZIP parsing: single, comma-separated, numeric ranges (e.g. 10001-10010)
 - per_zip_limit: collect up to N links per ZIP
 - index_ranges: select which 1-based index ranges per ZIP to actually scrape (e.g. "1-20,30-40")
 - preserve order & keep duplicates (default). set dedupe_links=True to dedupe.
 - dashboard endpoint added to satisfy templates using url_for('dashboard')
"""

import io
import logging
import random
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from typing import Iterable, List, Tuple, Union

import pandas as pd
import requests
import usaddress  # type: ignore
from flask import Flask, jsonify, render_template, request, send_file, abort
from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException,
                                        TimeoutException, WebDriverException)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Application State Management ---
APP_STATE = {
    "scraping_active": False,
    "stop_scraping_flag": False,
    "status_message": "Ready to begin! Configure settings and start scraping.",
    "link_collection_progress": 0.0,
    "detail_scraping_progress": 0.0,
    "link_count": 0,
    "scraped_count": 0,
    "total_to_scrape": 0,
    "results_df": pd.DataFrame(),
    "collected_links": [],  # list of (url, query, zipcode) in collection order
}
state_lock = threading.Lock()

# --- Constants ---
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
PHONE_REGEX = re.compile(r'(\+?\d[\d\s\-\(\)]{7,})')
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]

# --- Helper Functions ---


def update_status(message: str,
                  link_progress: float | None = None,
                  detail_progress: float | None = None,
                  link_count: int | None = None,
                  scraped_count: int | None = None,
                  total_to_scrape: int | None = None):
    """Thread-safe function to update the global APP_STATE."""
    with state_lock:
        APP_STATE["status_message"] = message
        if link_progress is not None:
            APP_STATE["link_collection_progress"] = round(float(link_progress or 0.0), 4)
        if detail_progress is not None:
            APP_STATE["detail_scraping_progress"] = round(float(detail_progress or 0.0), 4)
        if link_count is not None:
            APP_STATE["link_count"] = int(link_count)
        if scraped_count is not None:
            APP_STATE["scraped_count"] = int(scraped_count)
        if total_to_scrape is not None:
            APP_STATE["total_to_scrape"] = int(total_to_scrape)


def build_chrome(headless_mode=False):
    """Initializes and configures a Chrome WebDriver instance."""
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
    opts.add_argument("--log-level=3")
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option('useAutomationExtension', False)
    if headless_mode:
        opts.add_argument("--headless=new")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)


def retry_on_exception(max_retries=3, delay_seconds=5):
    """Decorator to retry a function call if a WebDriverException or requests exception occurs."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (WebDriverException, requests.exceptions.RequestException) as e:
                    logging.warning(f"Attempt {i + 1}/{max_retries} failed for {func.__name__}: {e}")
                    if i < max_retries - 1:
                        time.sleep(delay_seconds)
                    else:
                        raise
        return wrapper
    return decorator


def deobfuscate_email(text: str) -> str:
    text = (text or "").lower()
    text = text.replace('[at]', '@').replace('(at)', '@').replace(' at ', '@')
    text = text.replace('[dot]', '.').replace('(dot)', '.').replace(' dot ', '.')
    text = re.sub(r'\s+', '', text)
    return text


def find_emails(html_content: str) -> list[str]:
    """Return cleaned list of emails from a blob of text/HTML."""
    if not html_content:
        return []
    deobfuscated_html = deobfuscate_email(html_content)
    potential_emails = EMAIL_REGEX.findall(deobfuscated_html)

    blacklist_domains = {
        'example.com', 'w3.org', 'schema.org', 'maps.google.com', 'google.com',
        'facebook.com', 'instagram.com', 'twitter.com', 'x.com', 'linkedin.com'
    }
    blacklist_keywords = {'noreply', 'no-reply', 'privacy', 'support', 'postmaster', 'webmaster', 'abuse'}

    cleaned = []
    for email in potential_emails:
        e = email.lower().strip()
        if e.count('@') != 1:
            continue
        local, domain = e.split('@')
        if any(k in local for k in blacklist_keywords):
            continue
        if domain in blacklist_domains:
            continue
        if len(local) < 2 or len(domain) < 3:
            continue
        cleaned.append(e)
    # preserve order, remove exact duplicates while keeping first occurrence
    seen = set()
    ordered = []
    for e in cleaned:
        if e not in seen:
            seen.add(e)
            ordered.append(e)
    return ordered


def normalize_website(url: str) -> str:
    if not url: return ""
    url = url.strip()
    if "google.com/url?" in url:
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        if 'q' in qs and qs['q']:
            url = qs['q'][0]
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    return url.split("#")[0]


def extract_social_links(html_content: str) -> dict:
    """Extracts social media links from HTML robustly."""
    socials = {"Facebook": "", "Instagram": "", "Twitter": "", "LinkedIn": ""}
    patterns = {
        "Facebook": r'https?://(?:www\.)?facebook\.com/[^\s"\'<>]+',
        "Instagram": r'https?://(?:www\.)?instagram\.com/[^\s"\'<>]+',
        "Twitter": r'https?://(?:www\.)?(?:twitter|x)\.com/[^\s"\'<>]+',
        "LinkedIn": r'https?://(?:[a-z]{2,3}\.)?linkedin\.com/[^\s"\'<>]+',
    }
    for key, pat in patterns.items():
        matches = re.findall(pat, html_content, re.IGNORECASE)
        if matches:
            link = matches[0].split("?")[0].split("#")[0].rstrip("/")
            socials[key] = link
    return socials


def extract_phone_number(driver) -> str:
    """Extract phone number from Google Maps business page with fallbacks."""
    selectors = [
        'button[data-item-id^="phone:tel:"]',
        'button[data-item-id*="phone:"]',
        'a[href^="tel:"]',
        'div[aria-label^="Phone:"]',
        'div[jsaction*="phone"]'
    ]
    for sel in selectors:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, sel)
            phone = elem.text.strip()
            if phone and re.search(r'\d', phone):
                return phone
        except Exception:
            continue
    # Regex fallback
    matches = PHONE_REGEX.findall(driver.page_source)
    return matches[0] if matches else ""


def extract_emails_from_gmaps(driver) -> list[str]:
    """Try to get emails directly from the Google Maps business page."""
    return find_emails(driver.page_source)


def fetch_social_emails_via_requests(url: str) -> list[str]:
    try:
        r = requests.get(url, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=12)
        if r.status_code == 200:
            return find_emails(r.text)
    except Exception as e:
        logging.info(f"Requests failed for social URL {url}: {e}")
    return []


def fetch_social_emails_via_selenium(url: str, headless_mode: bool) -> list[str]:
    driver = None
    try:
        driver = build_chrome(headless_mode)
        driver.set_page_load_timeout(25)
        driver.get(url)
        time.sleep(4)
        return find_emails(driver.page_source)
    except Exception as e:
        logging.info(f"Selenium failed for social URL {url}: {e}")
        return []
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def scrape_social_emails(social_url: str, headless_mode: bool) -> list[str]:
    """Try to fetch emails from a FB/IG page, requests first, then selenium."""
    emails = fetch_social_emails_via_requests(social_url)
    if emails:
        return emails
    return fetch_social_emails_via_selenium(social_url, headless_mode)


@retry_on_exception(max_retries=2, delay_seconds=2)
def scrape_website_with_requests(website_url: str) -> tuple[list[str], dict]:
    """Fast, Selenium-free function to scrape a website for emails + socials."""
    if not website_url:
        return [], {}
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    session = requests.Session()
    try:
        response = session.get(website_url, headers=headers, timeout=12)
        response.raise_for_status()
        main_html = response.text

        emails = find_emails(main_html)
        socials = extract_social_links(main_html)

        if not emails:
            contact_keywords = ['contact', 'about', 'team', 'info', 'legal', 'privacy']
            link_pattern = re.compile(r'<a\s+(?:[^>]*?\s+)?href="([^"]+)"', re.IGNORECASE)
            potential_pages = set()
            for match in link_pattern.finditer(main_html):
                href = match.group(1)
                if any(kw in href.lower() for kw in contact_keywords):
                    full_url = urllib.parse.urljoin(website_url, href)
                    if urllib.parse.urlparse(full_url).netloc == urllib.parse.urlparse(website_url).netloc:
                        potential_pages.add(full_url)
            for page_url in list(potential_pages)[:3]:
                try:
                    page_resp = session.get(page_url, headers=headers, timeout=10)
                    emails.extend(find_emails(page_resp.text))
                    page_socials = extract_social_links(page_resp.text)
                    for k, v in page_socials.items():
                        if v:
                            socials[k] = socials.get(k) or v
                    if emails:
                        break
                except requests.RequestException:
                    continue

        # de-duplicate but keep order
        emails_ordered = []
        seen = set()
        for e in emails:
            if e not in seen:
                seen.add(e)
                emails_ordered.append(e)

        return emails_ordered, socials
    except requests.RequestException as e:
        logging.info(f"Requests scrape failed for {website_url}: {e}")
        return [], {}


@retry_on_exception(max_retries=2, delay_seconds=3)
def enhanced_website_email_scraping(website: str, headless_mode: bool) -> tuple[list[str], dict]:
    """Full Selenium website scrape."""
    driver = None
    try:
        driver = build_chrome(headless_mode)
        driver.set_page_load_timeout(25)
        driver.get(website)
        WebDriverWait(driver, 15).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        time.sleep(2)
        emails = find_emails(driver.page_source)
        socials = extract_social_links(driver.page_source)

        try:
            links = driver.find_elements(By.TAG_NAME, "a")
            checked = 0
            for link in links:
                if checked >= 3:
                    break
                href = link.get_attribute("href") or ""
                text = (link.text or "").lower()
                if any(k in href.lower() for k in ["contact", "about", "team", "info"]) or any(k in text for k in ["contact", "about", "team", "info"]):
                    try:
                        driver.get(href)
                        WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
                        time.sleep(1.2)
                        emails.extend(find_emails(driver.page_source))
                        page_socials = extract_social_links(driver.page_source)
                        for k, v in page_socials.items():
                            if v:
                                socials[k] = socials.get(k) or v
                        checked += 1
                    except Exception:
                        continue
        except Exception:
            pass

        # keep order & unique
        seen = set()
        ordered_emails = []
        for e in emails:
            if e not in seen:
                seen.add(e)
                ordered_emails.append(e)

        return ordered_emails, socials
    except Exception as e:
        logging.error(f"Error Selenium-scraping website {website}: {e}")
        return [], {}
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


@retry_on_exception()
def scrape_business_entry(gmaps_url: str, search_query_used: str, zipcode: str, timeout: int, headless_mode: bool) -> dict:
    gmaps_driver = None
    try:
        gmaps_driver = build_chrome(headless_mode)
        gmaps_driver.get(gmaps_url)
        WebDriverWait(gmaps_driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.DUwDvf, h1.lfPIob'))
        )
        time.sleep(random.uniform(1.5, 3.5))

        name = ""
        try:
            name = gmaps_driver.find_element(By.CSS_SELECTOR, 'h1.DUwDvf, h1.lfPIob').text.strip()
        except Exception:
            pass

        address = ""
        try:
            address = gmaps_driver.find_element(By.CSS_SELECTOR, 'button[data-item-id="address"]').text.strip()
        except Exception:
            pass

        category = ""
        try:
            category = gmaps_driver.find_element(By.CSS_SELECTOR, 'button[jsaction*="category"]').text.strip()
        except Exception:
            pass

        phone = extract_phone_number(gmaps_driver)

        # City/State parsing
        city, state = "", ""
        if address:
            try:
                tagged, _ = usaddress.tag(address)
                city = tagged.get('PlaceName', '')
                state = tagged.get('StateName', '')
            except Exception as e:
                logging.info(f"Address parse failed: {e}")

        # Get website if available
        website = ""
        try:
            a = gmaps_driver.find_element(By.CSS_SELECTOR, 'a[data-item-id="authority"]')
            website = normalize_website(a.get_attribute("href") or "")
        except Exception:
            # fallback: sometimes the website is inside a different selector
            try:
                anchors = gmaps_driver.find_elements(By.CSS_SELECTOR, 'a')
                for a in anchors:
                    href = a.get_attribute("href") or ""
                    if "http" in href and "google.com" not in href:
                        website = normalize_website(href)
                        break
            except Exception:
                website = ""

        maps_emails = set(extract_emails_from_gmaps(gmaps_driver))

        website_emails = set()
        socials = {}
        if website:
            req_emails, req_socials = scrape_website_with_requests(website)
            website_emails.update(req_emails)
            socials.update(req_socials)

            sel_emails, sel_socials = enhanced_website_email_scraping(website, headless_mode)
            website_emails.update(sel_emails)
            for k, v in sel_socials.items():
                if v and not socials.get(k):
                    socials[k] = v

        fb_emails, insta_emails = set(), set()
        if socials.get("Facebook"):
            fb_emails.update(scrape_social_emails(socials["Facebook"], headless_mode))
        if socials.get("Instagram"):
            insta_emails.update(scrape_social_emails(socials["Instagram"], headless_mode))

        all_emails = set()
        all_emails.update(maps_emails)
        all_emails.update(website_emails)
        all_emails.update(fb_emails)
        all_emails.update(insta_emails)

        final_emails = ", ".join(sorted(all_emails))
        email_count = len(all_emails)

        rating, review_count, plus_code = "", "", ""
        try:
            rating_el = gmaps_driver.find_element(By.CSS_SELECTOR, 'div.F7nice')
            txt = rating_el.text.strip().replace(',', '.')
            m1 = re.search(r'(\d[.,]\d+)', txt)
            if m1:
                rating = m1.group(1)
            m2 = re.search(r'\((\d{1,3}(?:[.,]\d{3})*)\)', txt)
            if m2:
                review_count = re.sub(r'[.,]', '', m2.group(1))
        except Exception:
            pass

        try:
            plus_code = gmaps_driver.find_element(By.CSS_SELECTOR, 'button[data-item-id="plus_code"]').text.strip()
        except Exception:
            plus_code = ""

        return {
            "Category": category, "City": city, "State": state,
            "Name": name, "Address": address, "Phone": phone,
            "Website": website,
            "Facebook": socials.get("Facebook", ""), "Instagram": socials.get("Instagram", ""),
            "Twitter": socials.get("Twitter", ""), "LinkedIn": socials.get("LinkedIn", ""),
            "Maps Email": ", ".join(sorted(maps_emails)),
            "Website Email": ", ".join(sorted(website_emails)),
            "Facebook Email": ", ".join(sorted(fb_emails)),
            "Instagram Email": ", ".join(sorted(insta_emails)),
            "Final Email": final_emails,
            "Email Count": email_count,
            "Source": "Website/Maps/Social" if final_emails else "",
            "Maps URL": gmaps_url,
            "Rating": rating, "Review Count": review_count, "Plus Code": plus_code,
            "Status": "SCRAPED", "Zipcode": zipcode.split(" in ")[0], "Search Query": search_query_used,
        }

    except Exception as e:
        logging.error(f"Error scraping {gmaps_url}: {e}", exc_info=True)
        return {"Maps URL": gmaps_url, "Status": f"SCRAPE ERROR: {str(e)[:200]}"}
    finally:
        if gmaps_driver:
            try:
                gmaps_driver.quit()
            except Exception:
                pass


# ---------------------------
# New: ZIP parsing & index range parsing
# ---------------------------

def parse_zipcodes(zip_input: Union[str, Iterable[str]]) -> List[str]:
    """Accepts: '10001,10002-10005,90001' or list of strings. Returns expanded list of zip strings."""
    if zip_input is None:
        return []
    if isinstance(zip_input, (list, tuple, set)):
        items = list(zip_input)
    else:
        # string: split by commas/newlines/spaces
        items = re.split(r'[,;\n]+', str(zip_input))
    out = []
    for it in items:
        s = str(it).strip()
        if not s:
            continue
        # range like 10001-10010
        m = re.match(r'^(\d+)\s*-\s*(\d+)$', s)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if a <= b:
                for z in range(a, b + 1):
                    out.append(str(z))
            else:
                for z in range(a, b - 1, -1):
                    out.append(str(z))
        else:
            out.append(s)
    return out


def parse_index_ranges(ranges_input: Union[str, Iterable[Union[str, Tuple[int, int]]], None]) -> List[Tuple[int, int]]:
    """
    Accepts:
     - "1-20,30-40"
     - ["1-20", "30-40"]
     - [(1,20), (30,40)]
    Returns list of (start,end) inclusive, 1-based ints.
    """
    if not ranges_input:
        return []
    out = []
    if isinstance(ranges_input, str):
        parts = re.split(r'[,;]+', ranges_input)
    elif isinstance(ranges_input, (list, tuple, set)):
        parts = list(ranges_input)
    else:
        return []
    for p in parts:
        if isinstance(p, tuple) and len(p) == 2:
            out.append((int(p[0]), int(p[1])))
            continue
        s = str(p).strip()
        if not s:
            continue
        m = re.match(r'^(\d+)\s*-\s*(\d+)$', s)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if a <= b:
                out.append((a, b))
            else:
                out.append((b, a))
        else:
            # single number
            if s.isdigit():
                n = int(s)
                out.append((n, n))
    return out


def filter_links_by_index_ranges(collected_links: list[tuple], index_ranges: list[tuple]) -> list[tuple]:
    """
    Keep links whose 1-based per-ZIP index lies within any of index_ranges.
    Example: collected_links = [(url, q, zip), ...]
    """
    if not index_ranges:
        return collected_links.copy()
    result = []
    counters = {}
    for url, q, z in collected_links:
        counters.setdefault(z, 0)
        counters[z] += 1
        occ = counters[z]
        keep = any(start <= occ <= end for (start, end) in index_ranges)
        if keep:
            result.append((url, q, z))
    return result


# ---------------------------
# Link collection and scraping logic
# ---------------------------

def collect_gmaps_links(config: dict):
    """
    Collect Google Maps place URLs given config.
    config keys used:
     - general_search_term (str)
     - categories (list[str])
     - zipcodes (list[str])
     - max_scrolls (int)
     - scroll_pause (int)
     - headless_mode (bool)
     - per_zip_limit (int or None)
     - dedupe_links (bool)
    """
    driver = None
    try:
        driver = build_chrome(config['headless_mode'])
        # build queries
        queries = [f"{config['general_search_term']} {cat} {zipc}".strip()
                   for cat in config['categories'] for zipc in config['zipcodes']]
        total_queries = len(queries)

        # zip iterator repeats zipcodes per category (same logic)
        zip_iter = (z for _ in config['categories'] for z in config['zipcodes'])

        for i, (query, zipc) in enumerate(zip(queries, zip_iter)):
            if APP_STATE["stop_scraping_flag"]:
                update_status("Link collection stopped by user.")
                return

            url = f"https://www.google.com/maps/search/{urllib.parse.quote(query)}"
            logging.info(f"Collecting: {query} -> {url}")
            try:
                driver.get(url)
            except Exception as e:
                logging.warning(f"Failed to load search URL for '{query}': {e}")
                continue

            try:
                feed = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]')))
            except TimeoutException:
                logging.warning(f"No results feed for '{query}'. Skipping.")
                continue

            per_zip_count = 0
            for scroll_idx in range(config['max_scrolls']):
                links_before = len(driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc'))
                # scroll the results feed container
                try:
                    driver.execute_script("arguments[0].scrollBy(0, 5000);", feed)
                except Exception:
                    # fallback to window scroll
                    try:
                        driver.execute_script("window.scrollBy(0, 4000);")
                    except Exception:
                        pass

                try:
                    WebDriverWait(driver, config['scroll_pause']).until(
                        lambda d: len(d.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')) > links_before or \
                                  "You've reached the end of the list" in d.page_source
                    )
                except TimeoutException:
                    logging.info(f"Scroll timeout for '{query}'. Ending scroll.")
                    break

                cards = driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
                for card in cards:
                    href = card.get_attribute("href")
                    if href and "/maps/place/" in href:
                        tup = (href, query, zipc)
                        # preserve order and duplicates by default; dedupe option available
                        with state_lock:
                            if config.get('dedupe_links'):
                                if tup not in APP_STATE["collected_links"]:
                                    APP_STATE["collected_links"].append(tup)
                                    per_zip_count += 1
                            else:
                                APP_STATE["collected_links"].append(tup)
                                per_zip_count += 1

                        # update link_count in state
                        with state_lock:
                            APP_STATE["link_count"] = len(APP_STATE["collected_links"])

                        # per-zip limit
                        if config.get('per_zip_limit') and per_zip_count >= int(config['per_zip_limit']):
                            logging.info(f"Reached per-zip limit ({config['per_zip_limit']}) for {zipc}")
                            break

                update_status(
                    f"Query {i + 1}/{total_queries}: '{query}'. Found {len(APP_STATE['collected_links'])} links.",
                    link_count=len(APP_STATE['collected_links'])
                )

                if config.get('per_zip_limit') and per_zip_count >= int(config['per_zip_limit']):
                    break

                if "You've reached the end of the list" in driver.page_source:
                    logging.info(f"End of results for '{query}'.")
                    break

            update_status(f"Finished query {i + 1}/{total_queries}.", link_progress=((i + 1) / max(1, total_queries)))

    except Exception as e:
        logging.error(f"Error during link collection: {e}", exc_info=True)
        update_status(f"Error during link collection: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def scrape_details(config: dict):
    """Scrape detailed pages for collected links, applying index ranges if given."""
    # Copy collected links safely
    with state_lock:
        collected = list(APP_STATE["collected_links"])

    # apply index ranges if any
    index_ranges = parse_index_ranges(config.get('index_ranges'))
    filtered_links = filter_links_by_index_ranges(collected, index_ranges)

    total_links = len(filtered_links)
    update_status(f"Scraping details for {total_links} businesses...", total_to_scrape=total_links)

    results = []
    completed = 0

    if total_links == 0:
        update_status("No links to scrape after filtering. Stage complete.")
        with state_lock:
            APP_STATE["results_df"] = pd.DataFrame()
        return

    with ThreadPoolExecutor(max_workers=int(config.get('max_workers', 3))) as pool:
        futures = {pool.submit(scrape_business_entry, url, query, zipc, int(config.get('scrape_timeout', 15)), config.get('headless_mode')): (url, query, zipc)
                   for url, query, zipc in filtered_links}

        for fut in as_completed(futures):
            if APP_STATE["stop_scraping_flag"]:
                update_status("Detail scraping stopped by user.")
                break
            try:
                result = fut.result()
            except Exception as e:
                logging.error(f"A worker raised an exception: {e}", exc_info=True)
                result = {"Maps URL": futures.get(fut, ("", "", ""))[0], "Status": f"WORKER ERROR: {str(e)[:150]}"}
            if result:
                results.append(result)
            completed += 1
            update_status(
                f"Scraped {completed}/{total_links} businesses...",
                detail_progress=(completed / total_links),
                scraped_count=len(results),
                total_to_scrape=total_links
            )

    # Build DataFrame preserving insertion order
    with state_lock:
        if results:
            desired_order = [
                "Category", "City", "State", "Name", "Address", "Phone", "Website",
                "Facebook", "Instagram", "Twitter", "LinkedIn",
                "Maps Email", "Website Email", "Facebook Email", "Instagram Email",
                "Final Email", "Email Count",
                "Source", "Maps URL", "Rating", "Review Count", "Plus Code",
                "Status", "Zipcode", "Search Query"
            ]
            df = pd.DataFrame(results)
            for col in desired_order:
                if col not in df.columns:
                    df[col] = ""
            APP_STATE["results_df"] = df[desired_order]
        else:
            APP_STATE["results_df"] = pd.DataFrame()


def scraping_worker(config: dict):
    """Main worker thread."""
    try:
        with state_lock:
            APP_STATE["scraping_active"] = True
            APP_STATE["stop_scraping_flag"] = False
            APP_STATE["results_df"] = pd.DataFrame()
            APP_STATE["collected_links"] = []
            APP_STATE["link_count"] = 0
            APP_STATE["scraped_count"] = 0
            APP_STATE["total_to_scrape"] = 0

        update_status("Stage 1/2: Collecting Google Maps URLs...")
        collect_gmaps_links(config)

        if APP_STATE["stop_scraping_flag"]:
            update_status("Scraping stopped.")
            return

        if not APP_STATE["collected_links"]:
            update_status("Stage 1 complete: No links found.")
            return

        update_status("Stage 2/2: Scraping business details...")
        scrape_details(config)
        if not APP_STATE["stop_scraping_flag"]:
            update_status("Scraping complete! View and download results.")
    except Exception as e:
        logging.error(f"Scraping worker failed: {e}", exc_info=True)
        update_status(f"An error occurred: {e}")
    finally:
        with state_lock:
            APP_STATE["scraping_active"] = False
            APP_STATE["stop_scraping_flag"] = False


# --- Flask Routes ---

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/dashboard')
def dashboard():
    # Minimal dashboard template (avoids BuildError in templates that expect this endpoint)
    return render_template('dashboard.html')


@app.route('/start-scraping', methods=['POST'])
def start_scraping():
    with state_lock:
        if APP_STATE["scraping_active"]:
            return jsonify({"status": "error", "message": "Scraping is already in progress."}), 400

    config = request.json or {}
    # normalize/validate config and fill defaults
    config_parsed = {
        'general_search_term': config.get('general_search_term', '').strip(),
        'categories': config.get('categories') or [''],
        'zipcodes': parse_zipcodes(config.get('zipcodes', [])),
        'max_scrolls': int(config.get('max_scrolls', 8)),
        'scroll_pause': int(config.get('scroll_pause', 2)),
        'max_workers': int(config.get('max_workers', 3)),
        'scrape_timeout': int(config.get('scrape_timeout', 15)),
        'headless_mode': bool(config.get('headless_mode', True)),
        'per_zip_limit': int(config['per_zip_limit']) if config.get('per_zip_limit') else None,
        'index_ranges': config.get('index_ranges'),  # parsed later
        'dedupe_links': bool(config.get('dedupe_links', False)),
    }

    # basic validation
    if not config_parsed['general_search_term']:
        return jsonify({"status": "error", "message": "general_search_term is required"}), 400
    if not config_parsed['zipcodes']:
        return jsonify({"status": "error", "message": "At least one zipcode required"}), 400

    thread = threading.Thread(target=scraping_worker, args=(config_parsed,))
    thread.daemon = True
    thread.start()
    return jsonify({"status": "success", "message": "Scraping process started."})


@app.route('/status')
def status():
    with state_lock:
        state_copy = APP_STATE.copy()
        state_copy.pop("results_df", None)
        return jsonify(state_copy)


@app.route('/stop-scraping', methods=['POST'])
def stop_scraping():
    with state_lock:
        if APP_STATE["scraping_active"]:
            APP_STATE["stop_scraping_flag"] = True
            return jsonify({"status": "success", "message": "Stop signal sent."})
        else:
            return jsonify({"status": "error", "message": "No scraping process is active."})


@app.route('/get-results')
def get_results():
    with state_lock:
        if not APP_STATE["results_df"].empty:
            return jsonify(APP_STATE["results_df"].to_dict(orient='records'))
        else:
            return jsonify([])


@app.route('/download-csv')
def download_csv():
    with state_lock:
        if not APP_STATE["results_df"].empty:
            buffer = io.BytesIO()
            APP_STATE["results_df"].to_csv(buffer, index=False, encoding='utf-8')
            buffer.seek(0)
            return send_file(
                buffer,
                as_attachment=True,
                download_name='scraped_businesses.csv',
                mimetype='text/csv'
            )
        return "No data to download.", 404


# --- Flask Entry Point ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
