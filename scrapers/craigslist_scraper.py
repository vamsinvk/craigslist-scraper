import requests
from bs4 import BeautifulSoup
import json, csv, time, os, re, sys, logging, random, threading
from datetime import datetime, timezone
from urllib.parse import urlparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ✅ Fixed deprecation warning
RUN_DATE  = os.environ.get("RUN_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
BASE_DIR  = os.path.join("FSBO", "Craigslist", "DATA", RUN_DATE)
CSV_DIR   = os.path.join(BASE_DIR, "CSV")
JSON_DIR  = os.path.join(BASE_DIR, "JSON")

os.makedirs(CSV_DIR,  exist_ok=True)
os.makedirs(JSON_DIR, exist_ok=True)

CONFIG = {
    "BASE_URL":           "https://cnj.craigslist.org/search/edison-nj/cta?lat=40.519&lon=-74.397&postedToday=1&search_distance=100",
    "OUTPUT_CSV":         os.path.join(CSV_DIR,  "craigslist_cars.csv"),
    "OUTPUT_JSON":        os.path.join(JSON_DIR, "craigslist_cars.json"),
    "CHECKPOINT_FILE":    os.path.join(BASE_DIR, "checkpoint.json"),
    "LOG_FILE":           os.path.join(BASE_DIR, "scraper.log"),
    "DELAY_MIN":          2.0,
    "DELAY_MAX":          3.5,
    "PAGE_DELAY":         1.2,
    "MAX_RETRIES":        3,
    "MAX_DOMAIN_WORKERS": 8,
    "STORM_THRESHOLD":    4,
    "COOLDOWN_SECONDS":   120,
    "OVERFLOW_WARN":      1217,   # warn if bracket has more than this
}

# ─────────────────────────────────────────────────────────────
# PRICE BRACKETS — each kept under 270 to avoid 312 cap
# Tight in the $3k–$10k peak zone based on histogram
# ─────────────────────────────────────────────────────────────
SEARCH_BRACKETS = [
    (0,      1000),
    (1001,   2000),
    (2001,   3000),
    # ⚡ Peak zone — very tight
    (3001,   3700),
    (3701,   4300),
    (4301,   4800),
    (4801,   5300),
    (5301,   5900),
    (5901,   6500),
    (6501,   7200),
    (7201,   8000),
    (8001,   9000),
    (9001,   10000),
    # Medium density
    (10001,  12000),
    (12001,  14500),
    (14501,  17500),
    (17501,  22000),
    # Low tail
    (22001,  35000),
    (35001,  999999),
]

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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(CONFIG["LOG_FILE"], encoding="utf-8"),
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
# THREAD-LOCAL SESSIONS
# ─────────────────────────────────────────────────────────────
_thread_local   = threading.local()
_primed_domains = set()
_prime_lock     = threading.Lock()

PROXY_URL = os.environ.get("PROXY_URL", "")

def get_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        if PROXY_URL:
            _thread_local.session.proxies.update({
                "http":  PROXY_URL,
                "https": PROXY_URL,
            })
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
        need_root = root not in _primed_domains
        if need_root:
            _primed_domains.add(root)
        need_sub = sub_url and sub_url not in _primed_domains
        if need_sub:
            _primed_domains.add(sub_url)

    if need_root:
        try:
            session.get(root, headers=get_headers(), timeout=8)
            log.info(f"Primed root: {root}")
        except Exception as e:
            log.warning(f"Prime failed {root}: {e}")
            with _prime_lock:
                _primed_domains.discard(root)

    if need_sub:
        try:
            time.sleep(0.3)
            session.get(sub_url, headers=get_headers(referer=root), timeout=8)
            log.info(f"Primed sub-area: {sub_url}")
        except Exception as e:
            log.warning(f"Sub-area prime failed {sub_url}: {e}")
            with _prime_lock:
                _primed_domains.discard(sub_url)

# ─────────────────────────────────────────────────────────────
# SAFE GET
# ─────────────────────────────────────────────────────────────
def safe_get(url):
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
# DOMAIN PROBE
# ─────────────────────────────────────────────────────────────
def probe_domain(domain):
    session = get_session()
    try:
        resp = session.get(f"https://{domain}/", headers=get_headers(), timeout=8)
        return resp.status_code == 200
    except:
        return False

# ─────────────────────────────────────────────────────────────
# PHASE 1 — Price bracket stub collection
# ─────────────────────────────────────────────────────────────
def scrape_bracket_stubs(min_price, max_price, seen_pids):
    stubs    = []
    start    = 0
    base_url = (
        f"https://cnj.craigslist.org/search/edison-nj/cta"
        f"?lat=40.519&lon=-74.397&search_distance=15"
        f"&min_price={min_price}&max_price={max_price}"
    )

    while True:
        url  = build_page_url(base_url, start)
        resp = safe_get(url)
        if not resp:
            log.error(f"  Failed: ${min_price}-${max_price} s={start}")
            break

        # ✅ Overflow check on first page
        if start == 0:
            m = re.search(r'"numberOfItems"\s*:\s*(\d+)', resp.text)
            bracket_total = int(m.group(1)) if m else 0
            log.info(f"  [${min_price}–${max_price}] Total in bracket: {bracket_total}")
            if bracket_total > CONFIG["OVERFLOW_WARN"]:
                log.warning(
                    f"  ⚠️  SPLIT THIS BRACKET — "
                    f"${min_price}–${max_price} has {bracket_total} listings!"
                )

        soup  = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("li.cl-static-search-result") or soup.select("li.result-row")
        if not cards:
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
            if listing_domain in IRRELEVANT_DOMAINS:
                continue

            pid = pid_from_url(listing_url)
            if not pid or pid in seen_pids:
                continue
            seen_pids.add(pid)
            new += 1

            title = card.get("title", "").strip()
            if not title:
                el    = card.select_one("a.posting-title, .title")
                title = el.get_text(strip=True) if el else a.get_text(strip=True)

            price_el    = card.select_one(".priceinfo") or card.select_one(".price")
            price       = price_el.get_text(strip=True) if price_el else ""

            location_el = card.select_one(".supertitle") or card.select_one(".location")
            location    = location_el.get_text(strip=True) if location_el else ""

            listing_type = "dealer" if "/ctd/" in listing_url else "owner"

            stubs.append({
                "run_date":     RUN_DATE,
                "pid":          pid,
                "url":          listing_url,
                "title":        title,
                "price":        price,
                "location":     location,
                "domain":       listing_domain,
                "listing_type": listing_type,
            })

        log.info(f"    s={start} | {new} new | bracket total: {len(stubs)}")

        if new == 0:
            break

        start += 16
        time.sleep(CONFIG["PAGE_DELAY"] + random.uniform(0, 0.6))

    return stubs


def scrape_search_stubs():
    # ✅ Clear stale checkpoint so old PIDs don't block new brackets
    if os.path.exists(CONFIG["CHECKPOINT_FILE"]):
        os.remove(CONFIG["CHECKPOINT_FILE"])
        log.info("Cleared stale checkpoint.")

    all_stubs = []
    seen_pids = set()

    main_session = get_session()
    prime_domain(CONFIG["BASE_URL"], main_session)

    log.info(f"Running {len(SEARCH_BRACKETS)} price brackets:")
    for mn, mx in SEARCH_BRACKETS:
        log.info(f"   ${mn:>6} – ${mx}")

    for min_price, max_price in SEARCH_BRACKETS:
        bracket_stubs = scrape_bracket_stubs(min_price, max_price, seen_pids)
        all_stubs.extend(bracket_stubs)
        log.info(
            f"  ✓ Bracket ${min_price}–${max_price}: "
            f"{len(bracket_stubs)} listings | Running total: {len(all_stubs)}"
        )
        time.sleep(random.uniform(1.5, 2.5))

    log.info(f"Phase 1 complete — {len(all_stubs)} total unique listings")
    return all_stubs

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

    desc_el = soup.select_one("#postingbody")
    if desc_el:
        for tag in desc_el.select(".print-qrcode-label, .qrcode-container"):
            tag.decompose()
    description = desc_el.get_text(separator=" ", strip=True) if desc_el else ""

    time_tags    = soup.select("time[datetime]")
    posted_time  = time_tags[0].get("datetime", "") if len(time_tags) > 0 else ""
    updated_time = time_tags[1].get("datetime", "") if len(time_tags) > 1 else ""

    lat, lon = "", ""
    map_el = soup.select_one("#map")
    if map_el:
        lat = map_el.get("data-latitude",  "")
        lon = map_el.get("data-longitude", "")

    images = [a.get("href", "") for a in soup.select("#thumbs a") if a.get("href")]
    if not images:
        images = [img.get("src", "") for img in soup.select(".slide img[src]")]

    post_id = stub.get("pid", "")
    for info in soup.select(".postinginfos .postinginfo"):
        t = info.get_text(strip=True)
        if "post id" in t.lower():
            post_id = re.sub(r"[^\d]", "", t)
            break

    core_keys  = {"run_date", "pid", "url", "title", "price", "location", "domain", "listing_type"}
    raw_attrs  = parse_attrs_html(soup)
    safe_attrs = {
        (f"attr_{k}" if k in core_keys else k): v
        for k, v in raw_attrs.items()
    }

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
        **safe_attrs,
    }

# ─────────────────────────────────────────────────────────────
# DOMAIN-PARALLEL SCRAPER
# ─────────────────────────────────────────────────────────────
results_lock = threading.Lock()
all_results  = []
done_count   = 0


def scrape_domain_batch(domain, stubs, total_all):
    global done_count
    consecutive_403 = 0

    for i, stub in enumerate(stubs):
        detail = scrape_detail(stub)
        failed = "error" in detail

        if failed:
            consecutive_403 += 1
            if consecutive_403 >= CONFIG["STORM_THRESHOLD"]:
                domain_alive = probe_domain(domain)
                if domain_alive:
                    log.info(f"  >> [{domain}] {consecutive_403}x 403 but domain UP → listings expired.")
                    consecutive_403 = 0
                else:
                    log.warning(f"  >> [{domain}] REAL rate limit — cooling {CONFIG['COOLDOWN_SECONDS']}s...")
                    time.sleep(CONFIG["COOLDOWN_SECONDS"])
                    consecutive_403 = 0
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
            should_save = current % 25 == 0
            snapshot    = list(all_results) if should_save else None

        if should_save and snapshot:
            good = [r for r in snapshot if "error" not in r]
            save_checkpoint(snapshot)
            save_csv(good)
            save_json(good)
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

    total       = len(stubs)
    num_workers = min(len(domain_groups), CONFIG["MAX_DOMAIN_WORKERS"])
    avg_delay   = (CONFIG["DELAY_MIN"] + CONFIG["DELAY_MAX"]) / 2
    max_batch   = max(len(v) for v in domain_groups.values())
    est_secs    = max_batch * avg_delay

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
# CHECKPOINT + CSV + JSON
# ─────────────────────────────────────────────────────────────
def load_checkpoint():
    if os.path.exists(CONFIG["CHECKPOINT_FILE"]):
        with open(CONFIG["CHECKPOINT_FILE"], "r", encoding="utf-8") as f:
            data = json.load(f)
            for r in data:
                if "listing_type" not in r:
                    r["listing_type"] = "dealer" if "/ctd/" in r.get("url", "") else "owner"
                if "run_date" not in r:
                    r["run_date"] = RUN_DATE
            log.info(f"Checkpoint loaded: {len(data)} rows.")
            return data
    return []


def save_checkpoint(data):
    with open(CONFIG["CHECKPOINT_FILE"], "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


KEEP_COLUMNS = [
    "run_date", "pid", "url", "domain", "listing_type",
    "title", "price", "year", "make", "model", "type",
    "odometer", "title_status", "condition", "cylinders",
    "fuel", "transmission", "drive", "paint_color", "vin",
    "location", "latitude", "longitude",
    "posted_time", "updated_time", "image_count",
    "description", "image_urls",
]

DROP_COLUMNS = {"post_id", "est._monthly_pmt"}


def save_csv(data):
    if not data:
        return
    all_keys = set()
    for row in data:
        all_keys.update(row.keys())
    final_headers  = [c for c in KEEP_COLUMNS if c in all_keys]
    final_headers += sorted([k for k in all_keys if k not in KEEP_COLUMNS and k not in DROP_COLUMNS])
    with open(CONFIG["OUTPUT_CSV"], "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=final_headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
    log.info(f"CSV saved: {CONFIG['OUTPUT_CSV']} ({len(data)} rows, {len(final_headers)} cols)")


def save_json(data):
    if not data:
        return
    with open(CONFIG["OUTPUT_JSON"], "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log.info(f"JSON saved: {CONFIG['OUTPUT_JSON']} ({len(data)} rows)")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    start_time = datetime.now()
    log.info("=" * 60)
    log.info("Craigslist Scraper — Domain-Parallel | Final Version")
    log.info(f"Run Date : {RUN_DATE}")
    log.info(f"CSV      : {CONFIG['OUTPUT_CSV']}")
    log.info(f"JSON     : {CONFIG['OUTPUT_JSON']}")
    log.info(f"Start    : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    log.info("PHASE 1: Collecting listing URLs from search pages...")
    stubs = scrape_search_stubs()
    log.info(f"Total: {len(stubs)} | Remaining: {len(stubs)}")

    log.info("PHASE 2: Domain-parallel detail scraping...")
    scrape_domain_parallel(stubs)

    # Retry all failed listings once
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
    save_json(good)

    elapsed = datetime.now() - start_time
    log.info("=" * 60)
    log.info(f"DONE in {elapsed}")
    log.info(f"  Run Date         : {RUN_DATE}")
    log.info(f"  Total attempted  : {len(all_results)}")
    log.info(f"  ✓ Saved          : {len(good)}")
    log.info(f"      → Owner      : {len(owners)}")
    log.info(f"      → Dealer     : {len(dealers)}")
    log.info(f"  ✗ Expired/gone   : {len(expired)}")
    log.info(f"  CSV              : {CONFIG['OUTPUT_CSV']}")
    log.info(f"  JSON             : {CONFIG['OUTPUT_JSON']}")
    log.info("=" * 60)
