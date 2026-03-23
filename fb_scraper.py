import json
import os
import csv
import threading
from apify_client import ApifyClient
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---
APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")
if not APIFY_API_TOKEN:
    raise ValueError("APIFY_API_TOKEN environment variable not set!")

RUN_DATE = os.environ.get("RUN_DATE", datetime.utcnow().strftime("%Y-%m-%d"))

BASE_DATA_DIR      = os.path.join("FSBO", "Facebook", "DATA", RUN_DATE)
JSON_DIR           = os.path.join(BASE_DATA_DIR, "JSON")
CSV_DIR            = os.path.join(BASE_DATA_DIR, "CSV")

os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(CSV_DIR,  exist_ok=True)

DATABASE_JSON_PATH = os.path.join(JSON_DIR, "edison_processed_final_database.json")
DATABASE_CSV_PATH  = os.path.join(CSV_DIR,  "edison_processed_final_database.csv")

# ─────────────────────────────────────────────────────────────
# PRICE BRACKETS
# ─────────────────────────────────────────────────────────────
PRICE_BRACKETS = [
    {"min": 0,     "max": 2000},
    {"min": 2001,  "max": 5000},
    {"min": 5001,  "max": 10000},
    {"min": 10001, "max": 15000},
    {"min": 15001, "max": 25000},
    {"min": 25001, "max": 100000},
]

# ─────────────────────────────────────────────────────────────
# LOAD EXISTING DATABASE
# ─────────────────────────────────────────────────────────────
if os.path.exists(DATABASE_JSON_PATH):
    with open(DATABASE_JSON_PATH, "r", encoding="utf-8") as f:
        master_db = json.load(f)
    print(f"Loaded existing database: {len(master_db)} records.")
else:
    master_db = {}
    print("No existing database. Starting fresh.")

# Shared state — protected by db_lock
db_lock        = threading.Lock()
new_cars_added = 0
prices_updated = 0
spam_blocked   = 0
current_date   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ─────────────────────────────────────────────────────────────
# COLUMNS
# ─────────────────────────────────────────────────────────────
COLUMNS_TO_DROP = [
    "is_sold", "is_pending", "is_live",
    "__isMarketplaceListingRenderable", "__isMarketplaceListingWithChildListings",
    "__isMarketplaceListingWithDeliveryOptions", "__typename", "c2c_shipping_eligible",
    "can_buyer_make_checkout_offer", "condition", "creation_time_ms", "currency",
    "custom_sub_titles_with_rendering_flags", "dataType", "dealership_name",
    "delivery_types", "energy_efficiency_class_eu", "extraListingData",
    "fair_market_value_data", "formatted_price", "if_gk_just_listed_tag_on_search_feed",
    "inventory_count", "is_buy_now_enabled", "is_cart_enabled", "is_checkout_enabled",
    "is_draft", "is_email_communication_enabled", "is_hidden", "is_purchase_protected",
    "is_shipping_offered", "is_viewer_seller", "legal_disclosure_impressum_url",
    "legal_reporting_cta_type", "legal_reporting_uri", "listing_date", "listing_date_ms",
    "listing_inventory_type", "listing_is_rejected", "listing_video",
    "location_vanity_or_id", "logging_id", "marketplace_bump_info",
    "marketplace_listing_category_id", "marketplace_listing_category_name",
    "marketplace_listing_leaf_vt_category_name", "marketplace_listing_seller",
    "marketplace_listing_title", "marketplace_listing_virtual_taxonomy_category",
    "max_listing_price", "messaging_enabled", "min_listing_price", "origin_group",
    "parent_listing", "payment_time_period", "personalization_info", "photos",
    "primary_listing_photo", "product_feedback", "search_pivots", "seller",
    "seller_phone_number", "shipping_offered", "strikethrough_price", "subtitles",
    "sweepstake_enabled", "sweepstake_status", "vacation_mode", "vehicle_carfax_report",
    "vehicle_condition", "vehicle_features", "vehicle_identification_number",
    "vehicle_interior_color", "vehicle_is_paid_off", "vehicle_number_of_owners",
    "vehicle_registration_plate_information", "vehicle_specifications",
    "vehicle_trim_display_name", "vehicle_website_link",
]

DEALER_KEYWORDS = [
    "financ", "bad credit", "credit", "down payment",
    "dealership", "dealer", "bhph", "doc fee", "otd pricing",
]

# ─────────────────────────────────────────────────────────────
# BRACKET WORKER — runs in parallel
# ─────────────────────────────────────────────────────────────
def scrape_bracket(bracket):
    global new_cars_added, prices_updated, spam_blocked

    # Each thread gets its own client instance
    client = ApifyClient(APIFY_API_TOKEN)

    label = f"${bracket['min']}–${bracket['max']}"
    print(f"  ▶ Started  {label}")

    run_input = {
        "discordNotifyOnlyNew":  True,
        "enableDeduplication":   False,
        "fetchDetailedItems":    True,    # ✅ Full detailed data
        "location":              "edison",
        "radius":                "40000",
        "maxListingAge":         "86400",
        "maxResults":            250,
        "priceMin":              bracket["min"],
        "priceMax":              bracket["max"],
        "proxy": {
            "useApifyProxy":     True,
            "apifyProxyGroups":  ["RESIDENTIAL"],
            "apifyProxyCountry": "US",
        },
        "sortBy": "creation_time_descend",
    }

    try:
        run = client.actor("raidr-api/facebook-marketplace-vehicle-scraper").call(
            run_input=run_input,
            memory_mbytes=512,            # ✅ Reduced from 1024 → 512
        )

        dataset_items = client.dataset(run["defaultDatasetId"]).list_items().items
        print(f"  ✓ Done     {label} → {len(dataset_items)} listings")

        local_new = local_updated = local_spam = 0

        for item in dataset_items:
            car_data = item.get("extraListingData") or item

            car_id = car_data.get("id")
            if not car_id:
                continue

            current_price              = car_data.get("price")
            car_data["contact_url"]    = car_data.get("share_uri", f"https://www.facebook.com/marketplace/item/{car_id}")
            car_data["last_seen_date"] = current_date
            car_data["run_date"]       = RUN_DATE

            # Status
            if car_data.get("is_sold"):
                car_data["status"] = "sold"
            elif car_data.get("is_pending"):
                car_data["status"] = "pending"
            else:
                car_data["status"] = "live"

            # Drop columns
            for col in COLUMNS_TO_DROP:
                car_data.pop(col, None)

            # Dealer filter
            raw_desc   = str(car_data.get("description", ""))
            desc_lower = raw_desc.lower()
            car_data["is_likely_dealer"] = any(kw in desc_lower for kw in DEALER_KEYWORDS)

            # Dedup key
            clean_desc = raw_desc.strip()
            dedup_key  = clean_desc if len(clean_desc) > 10 else str(car_id)

            # Thread-safe write
            with db_lock:
                if dedup_key in master_db:
                    old_price = master_db[dedup_key].get("price")
                    if str(old_price) != str(current_price):
                        print(f"  💰 {car_data.get('title')}: ${old_price} → ${current_price}")
                        master_db[dedup_key]["price"]          = current_price
                        master_db[dedup_key]["last_seen_date"] = current_date
                        local_updated += 1
                    else:
                        local_spam += 1
                else:
                    car_data["first_seen_date"] = current_date
                    master_db[dedup_key]        = car_data
                    local_new += 1

        with db_lock:
            new_cars_added += local_new
            prices_updated += local_updated
            spam_blocked   += local_spam

        return {"label": label, "count": len(dataset_items), "error": None}

    except Exception as e:
        print(f"  ❌ Failed  {label}: {e}")
        return {"label": label, "count": 0, "error": str(e)}


# ─────────────────────────────────────────────────────────────
# MAIN — PARALLEL EXECUTION
# ─────────────────────────────────────────────────────────────
print(f"\n{'='*50}")
print(f"  Facebook Scraper — Parallel Edition")
print(f"  Run Date : {RUN_DATE}")
print(f"  Brackets : {len(PRICE_BRACKETS)} running simultaneously")
print(f"  RAM/actor: 512 MB")
print(f"{'='*50}\n")

start_time = datetime.now()

with ThreadPoolExecutor(max_workers=len(PRICE_BRACKETS)) as executor:
    futures = {executor.submit(scrape_bracket, b): b for b in PRICE_BRACKETS}
    for future in as_completed(futures):
        result = future.result()
        if result["error"]:
            print(f"  ⚠️  {result['label']} failed: {result['error']}")

elapsed = datetime.now() - start_time

# --- SAVE TO JSON ---
with open(DATABASE_JSON_PATH, "w", encoding="utf-8") as f:
    json.dump(master_db, f, indent=4, ensure_ascii=False)
print(f"\n✅ JSON saved: {DATABASE_JSON_PATH}")

# --- SAVE TO CSV ---
if master_db:
    all_keys, seen_keys = [], set()
    for car in master_db.values():
        for k in car.keys():
            if k not in seen_keys:
                all_keys.append(k)
                seen_keys.add(k)

    priority_headers = [
        "status", "title", "price", "is_likely_dealer",
        "contact_url", "run_date", "first_seen_date", "last_seen_date", "id",
    ]
    final_headers = priority_headers + [h for h in all_keys if h not in priority_headers]

    with open(DATABASE_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=final_headers, extrasaction="ignore")
        writer.writeheader()
        for car in master_db.values():
            clean_row = {
                k: (str(v) if isinstance(v, (list, dict)) else v)
                for k, v in car.items()
            }
            writer.writerow(clean_row)

    print(f"✅ CSV saved: {DATABASE_CSV_PATH}")

# --- SUMMARY ---
print(f"""
{'='*50}
  Run Date    : {RUN_DATE}
  ⏱  Elapsed  : {elapsed}
  Total in DB : {len(master_db)}
  New today   : {new_cars_added}
  Price drops : {prices_updated}
  Spam blocked: {spam_blocked}
  JSON        : {DATABASE_JSON_PATH}
  CSV         : {DATABASE_CSV_PATH}
{'='*50}
""")
