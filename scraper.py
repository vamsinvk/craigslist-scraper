# ============================================================
# Craigslist Car Scraper — Complete Final Version
# - Domain-parallel (1 worker per domain, human-pace per domain)
# - Thread-local sessions (no cookie race conditions)
# - Sub-area priming (/brk/, /lgi/, /wch/ etc.)
# - listing_type: owner vs dealer
# - Smart 403 detection (expired vs real block)
# - Checkpoint + retry on failure
# ============================================================

import requests
from bs4 import BeautifulSoup
import json, csv, time, os, re, sys, logging, random, threading
from datetime import datetime
from urllib.parse import urlparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
CONFIG = {
    "BASE_URL":           "https://cnj.craigslist.org/search/edison-nj/cta?lat=40.5385&lon=-74.3959&postedToday=1&search_distance=90",
    "OUTPUT_CSV":         "craigslist_cars.csv",
    "CHECKPOINT_FILE":    "checkpoint.json",
    "DELAY_MIN":          2.0,
    "DELAY_MAX":          3.5,
    "PAGE_DELAY":         1.2,
    "MAX_RETRIES":        3,
    "MAX_DOMAIN_WORKERS": 8,
    "STORM_THRESHOLD":    4,
    "COOLDOWN_SECONDS":   120,
}

IRRELEVANT_DOMAINS = {
    "chicago.craigslist.org",
    "losangeles.craigslist.org",
    "miami.craigslist.org",
    "phoenix.craigslist.org",
    "washingtondc.craigslist.org",
    "seattle.craigslist.org",
    "dallas.craigslist.org",
    "houston.craigslist.org",
    "atlanta.craigslist.org",
    "denver.craigslist.org",
}

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
#os.makedirs(os.path.dirname(CONFIG["OUTPUT_CSV"]),      exist_ok=True)
#os.makedirs(os.path.dirname(CONFIG["CHECKPOINT_FILE"]), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("craper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def get_headers(referer=None):
    return {
        "User-Agent":                random.choice(USER_AGENTS),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control":             "max-age=0",
        "Referer":                   referer or "https://craigslist.org/",
        "sec-ch-ua":                 '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "sec-ch-ua-mobile":          "?0",
        "sec-ch-ua-platform":        '"Windows"',
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "same-origin",
        "Sec-Fetch-User":            "?1",
    }

# ─────────────────────────────────────────────────────────────
# THREAD-LOCAL SESSIONS — each domain worker gets its own
# isolated session so cookies never cross-contaminate
# ─────────────────────────────────────────────────────────────
_thread_local   = threading.local()
_primed_domains = set()
_prime_lock     = threading.Lock()

def get_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def get_domain(url):
    try:
        return urlparse(url).netloc
    except:
        return "unknown"

def pid_from_url(url):
    m = re.search(r'/(\d{8,})(?:\.html)?', url or "")
    return m.group(1) if m else ""

def extract_sub_area(url):
    """Extract sub-area slug like brk, lgi, wch, que, brx, stn etc."""
    m = re.match(r'https?://[^/]+/([a-z]{2,5})/', url or "")
    if m:
        area = m.group(1)
        if area not in ("cto", "ctd", "cta", "d"):
            return area
    return None

def build_page_url(base, start):
    return f"{base}&s={start}" if "?" in base else f"{base}?s={start}"

# ─────────────────────────────────────────────────────────────
# DOMAIN + SUB-AREA PRIMING
# ─────────────────────────────────────────────────────────────
def prime_domain(url, session=None):
    if session is None:
        session = get_session()
    try:
        parsed   = urlparse(url)
        root     = f"{parsed.scheme}://{parsed.netloc}/"
        sub_area = extract_sub_area(url)
        sub_url  = f"{root}{sub_area}/" if sub_area else None
    except:
        return

    with _prime_lock:
        if root not in _primed_domains:
            try:
                session.get(root, headers=get_headers(), timeout=8)
                _primed_domains.add(root)
                log.info(f"Primed root: {root}")
            except Exception as e:
                log.warning(f"Prime failed {root}: {e}")

        if sub_url and sub_url not in _primed_domains:
            try:
                time.sleep(0.3)
                session.get(sub_url, headers=get_headers(referer=root), timeout=8)
                _primed_domains.add(sub_url)
                log.info(f"Primed sub-area: {sub_url}")
            except Exception as e:
                log.warning(f"Sub-area prime failed {sub_url}: {e}")

# ─────────────────────────────────────────────────────────────
# SAFE GET — thread-local session, correct sub-area referer
# ─────────────────────────────────────────────────────────────
def safe_get(url, use_main_session=False):
    session  = get_session()
    parsed   = urlparse(url)
    root     = f"{parsed.scheme}://{parsed.netloc}/"
    sub_area = extract_sub_area(url)
    referer  = f"{root}{sub_area}/" if sub_area else root

    prime_domain(url, session)

    for attempt in range(1, CONFIG["MAX_RETRIES"] + 1):
        try:
            resp = session.get(url, headers=get_headers(referer=referer), timeout=20)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 403:
                log.warning(f"403: {url}")
                return None
            elif resp.status_code == 429:
                wait = 40 * attempt
                log.warning(f"429 rate limit — sleeping {wait}s")
                time.sleep(wait)
            else:
                log.warning(f"HTTP {resp.status_code} attempt {attempt}: {url}")
                time.sleep(4 * attempt)
        except requests.exceptions.Timeout:
            time.sleep(6 * attempt)
        except requests.exceptions.ConnectionError:
            time.sleep(10 * attempt)
    log.error(f"Gave up: {url}")
    return None

# ─────────────────────────────────────────────────────────────
# DOMAIN PROBE — tells us if 403 is rate-limit vs expired listing
# ─────────────────────────────────────────────────────────────
def probe_domain(domain):
    session = get_session()
    try:
        resp = session.get(
            f"https://{domain}/",
            headers=get_headers(),
            timeout=8
        )
        return resp.status_code == 200
    except:
        return False

# ─────────────────────────────────────────────────────────────
# PHASE 1 — Collect stub URLs from search result pages
# ─────────────────────────────────────────────────────────────
def scrape_search_stubs():
    stubs     = []
    seen_pids = set()
    start     = 0
    total     = 9999

    # Use main session for search pages (single-threaded phase)
    main_session = get_session()
    prime_domain(CONFIG["BASE_URL"], main_session)

    while True:
        url  = build_page_url(CONFIG["BASE_URL"], start)
        log.info(f"[Search] s={start}")
        resp = safe_get(url)
        if not resp:
            log.error(f"Failed to load search page s={start}")
            break

        if start == 0:
            m = re.search(r'"numberOfItems"\s*:\s*(\d+)', resp.text)
            if m:
                total = int(m.group(1))
                log.info(f"Total reported by Craigslist: {total}")

        soup  = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("li.cl-static-search-result") or soup.select("li.result-row")
        if not cards:
            log.info("No listing cards found — search pages done.")
            break

        new = 0
        for card in cards:
            a = card.select_one("a[href]")
            if not a:
                continue
            listing_url = a.get("href", "").strip()
            if not listing_url:
                continue

            listing_domain = get_domain(listing_url)

            # Skip irrelevant national domains
            if listing_domain in IRRELEVANT_DOMAINS:
                continue

            pid = pid_from_url(listing_url)
            if not pid or pid in seen_pids:
                continue
            seen_pids.add(pid)
            new += 1

            # Title
            title = card.get("title", "").strip()
            if not title:
                el    = card.select_one(".title")
                title = el.get_text(strip=True) if el else a.get_text(strip=True)

            # Price
            price = ""
            el = card.select_one(".price")
            if el:
                price = el.get_text(strip=True)

            # Location
            location = ""
            el = card.select_one(".location")
            if el:
                location = el.get_text(strip=True)

            # Owner vs Dealer
            listing_type = "dealer" if "/ctd/" in listing_url else "owner"

            stubs.append({
                "pid":          pid,
                "url":          listing_url,
                "title":        title,
                "price":        price,
                "location":     location,
                "domain":       listing_domain,
                "listing_type": listing_type,
            })

        log.info(f"  s={start} | {new} new | Total stubs: {len(stubs)}")

        if new == 0 or len(stubs) >= total:
            break

        start += 120
        time.sleep(CONFIG["PAGE_DELAY"] + random.uniform(0, 0.6))

    return stubs

# ─────────────────────────────────────────────────────────────
# PHASE 2 — Parse detail page HTML
# ─────────────────────────────────────────────────────────────
def parse_attrs_html(soup):
    attrs       = {}
    attr_groups = soup.select("p.attrgroup, div.attrgroup")

    if len(attr_groups) >= 1:
        for span in attr_groups[0].select("span"):
            text  = span.get_text(strip=True)
            links = span.select("a[href]")
            if re.match(r"^\d{4}$", text):
                attrs["year"] = text
            elif len(links) >= 2:
                attrs["make"]  = links[0].get_text(strip=True)
                attrs["model"] = links[1].get_text(strip=True)
            elif len(links) == 1:
                if "make" not in attrs:
                    attrs["make"]  = links[0].get_text(strip=True)
                else:
                    attrs["model"] = links[0].get_text(strip=True)

    if len(attr_groups) >= 2:
        spans = attr_groups[1].select("span")
        i = 0
        while i < len(spans):
            text = spans[i].get_text(strip=True)
            if text.endswith(":"):
                key   = text.rstrip(":").strip().lower().replace(" ", "_")
                value = spans[i + 1].get_text(strip=True) if i + 1 < len(spans) else ""
                i += 2
                if key:
                    attrs[key] = value
            else:
                if text:
                    attrs[text.lower().replace(" ", "_")] = "yes"
                i += 1
    return attrs

def scrape_detail(stub):
    url  = stub.get("url", "")
    resp = safe_get(url)
    if not resp:
        return {**stub, "error": "fetch_failed"}

    soup = BeautifulSoup(resp.text, "html.parser")

    # Description
    desc_el = soup.select_one("#postingbody")
    if desc_el:
        for tag in desc_el.select(".print-qrcode-label, .qrcode-container"):
            tag.decompose()
    description = desc_el.get_text(separator=" ", strip=True) if desc_el else ""

    # Timestamps
    time_tags    = soup.select("time[datetime]")
    posted_time  = time_tags[0].get("datetime", "") if len(time_tags) > 0 else ""
    updated_time = time_tags[1].get("datetime", "") if len(time_tags) > 1 else ""

    # GPS
    lat, lon = "", ""
    map_el = soup.select_one("#map")
    if map_el:
        lat = map_el.get("data-latitude",  "")
        lon = map_el.get("data-longitude", "")

    # Images
    images = [a.get("href", "") for a in soup.select("#thumbs a") if a.get("href")]
    if not images:
        images = [img.get("src", "") for img in soup.select(".slide img[src]")]

    # Post ID
    post_id = stub.get("pid", "")
    for info in soup.select(".postinginfos .postinginfo"):
        t = info.get_text(strip=True)
        if "post id" in t.lower():
            post_id = re.sub(r"[^\d]", "", t)
            break

    return {
        **stub,
        "post_id":      post_id,
        "posted_time":  posted_time,
        "updated_time": updated_time,
        "latitude":     lat,
        "longitude":    lon,
        "description":  description,
        "image_count":  len(images),
        "image_urls":   " | ".join(images),
        **parse_attrs_html(soup),
    }

# ─────────────────────────────────────────────────────────────
# DOMAIN-PARALLEL SCRAPER
# ─────────────────────────────────────────────────────────────
results_lock = threading.Lock()
all_results  = []
done_count   = 0

def scrape_domain_batch(domain, stubs, total_all):
    global done_count
    consecutive_403  = 0

    for i, stub in enumerate(stubs):
        detail = scrape_detail(stub)
        failed = "error" in detail

        if failed:
            consecutive_403 += 1
            if consecutive_403 >= CONFIG["STORM_THRESHOLD"]:
                # Probe to distinguish expired listing vs real rate-limit
                domain_alive = probe_domain(domain)
                if domain_alive:
                    # Domain is up → these are just deleted/expired listings
                    log.info(
                        f"  >> [{domain}] {consecutive_403}x 403 but domain is UP "
                        f"→ listings expired/deleted. Continuing."
                    )
                    consecutive_403 = 0
                else:
                    # Domain blocking us → real rate limit
                    log.warning(
                        f"  >> [{domain}] REAL rate limit — "
                        f"cooling down {CONFIG['COOLDOWN_SECONDS']}s..."
                    )
                    time.sleep(CONFIG["COOLDOWN_SECONDS"])
                    consecutive_403 = 0
                    # Retry after cooldown
                    detail = scrape_detail(stub)
                    failed = "error" in detail
                    if not failed:
                        log.info(f"  >> [{domain}] Recovered after cooldown ✓")
        else:
            consecutive_403 = 0

        with results_lock:
            all_results.append(detail)
            done_count += 1
            current = done_count
            status  = "✓" if not failed else "✗ expired"
            log.info(
                f"[{current}/{total_all}] {status} [{domain}] "
                f"[{stub.get('listing_type','?')}] "
                f"{stub['title']} — {stub['price']}"
            )
            if current % 25 == 0:
                good = [r for r in all_results if "error" not in r]
                save_checkpoint(all_results)
                save_csv(good)
                log.info(f"  >> Checkpoint {current}/{total_all} | Clean: {len(good)}")

        delay = random.uniform(CONFIG["DELAY_MIN"], CONFIG["DELAY_MAX"])
        if (i + 1) % 10 == 0:
            delay += random.uniform(1.5, 3.0)
            log.info(f"  >> [{domain}] extended pause {delay:.1f}s")
        time.sleep(delay)


def scrape_domain_parallel(stubs):
    if not stubs:
        log.info("Nothing remaining to scrape.")
        return

    domain_groups = defaultdict(list)
    for stub in stubs:
        domain_groups[stub["domain"]].append(stub)

    total      = len(stubs)
    num_workers = min(len(domain_groups), CONFIG["MAX_DOMAIN_WORKERS"])
    avg_delay  = (CONFIG["DELAY_MIN"] + CONFIG["DELAY_MAX"]) / 2
    max_batch  = max(len(v) for v in domain_groups.values())
    est_secs   = max_batch * avg_delay

    log.info(f"Domains: {list(domain_groups.keys())}")
    log.info(f"Workers: {num_workers} | Longest domain: {max_batch} listings")
    log.info(f"Estimated time: ~{est_secs/60:.1f} min")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(scrape_domain_batch, domain, domain_stubs, total): domain
            for domain, domain_stubs in domain_groups.items()
        }
        for future in as_completed(futures):
            domain = futures[future]
            try:
                future.result()
                log.info(f"  >> Domain complete: {domain}")
            except Exception as e:
                log.error(f"  >> Domain error [{domain}]: {e}")

# ─────────────────────────────────────────────────────────────
# CHECKPOINT + CSV
# ─────────────────────────────────────────────────────────────
def load_checkpoint():
    if os.path.exists(CONFIG["CHECKPOINT_FILE"]):
        with open(CONFIG["CHECKPOINT_FILE"], "r", encoding="utf-8") as f:
            data = json.load(f)
            # Backfill listing_type on old checkpoint rows
            for r in data:
                if "listing_type" not in r:
                    r["listing_type"] = "dealer" if "/ctd/" in r.get("url", "") else "owner"
            log.info(f"Checkpoint loaded: {len(data)} rows.")
            return data
    return []

def save_checkpoint(data):
    with open(CONFIG["CHECKPOINT_FILE"], "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def save_csv(data):
    if not data:
        return
    all_keys, seen_keys = [], set()
    for row in data:
        for k in row.keys():
            if k not in seen_keys:
                all_keys.append(k)
                seen_keys.add(k)
    with open(CONFIG["OUTPUT_CSV"], "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
    log.info(f"CSV saved: {CONFIG['OUTPUT_CSV']} ({len(data)} rows)")

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    start_time = datetime.now()
    log.info("=" * 60)
    log.info("Craigslist Scraper — Domain-Parallel | Final Version")
    log.info(f"URL   : {CONFIG['BASE_URL']}")
    log.info(f"Start : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # Load checkpoint
    completed      = load_checkpoint()
    completed_pids = {r["pid"] for r in completed}
    all_results.extend(completed)
    done_count = len(completed)

    # Phase 1 — collect stubs
    log.info("PHASE 1: Collecting listing URLs from search pages...")
    stubs     = scrape_search_stubs()
    remaining = [s for s in stubs if s["pid"] not in completed_pids]
    log.info(f"Total: {len(stubs)} | Already done: {len(completed)} | Remaining: {len(remaining)}")

    # Phase 2 — domain-parallel detail scraping
    log.info("PHASE 2: Domain-parallel detail scraping...")
    scrape_domain_parallel(remaining)

    # Retry all failed listings once after full run
    failed_stubs = [r for r in all_results if "error" in r]
    if failed_stubs:
        log.info(f"Retrying {len(failed_stubs)} failed listings after 60s cooldown...")
        time.sleep(60)
        for stub in failed_stubs:
            detail = scrape_detail(stub)
            if "error" not in detail:
                for j, r in enumerate(all_results):
                    if r["pid"] == detail["pid"]:
                        all_results[j] = detail
                        log.info(f"  >> Recovered: {stub['title']}")
                        break
            time.sleep(random.uniform(3.0, 5.0))

    # Final save
    good    = [r for r in all_results if "error" not in r]
    expired = [r for r in all_results if "error" in r]
    dealers = [r for r in good if r.get("listing_type") == "dealer"]
    owners  = [r for r in good if r.get("listing_type") == "owner"]

    save_checkpoint(all_results)
    save_csv(good)

    elapsed = datetime.now() - start_time
    log.info("=" * 60)
    log.info(f"DONE in {elapsed}")
    log.info(f"  Total attempted  : {len(all_results)}")
    log.info(f"  ✓ Saved to CSV   : {len(good)}")
    log.info(f"      → Owner       : {len(owners)}")
    log.info(f"      → Dealer      : {len(dealers)}")
    log.info(f"  ✗ Expired/gone   : {len(expired)}")
    log.info(f"  Output           : {CONFIG['OUTPUT_CSV']}")
    log.info("=" * 60)
