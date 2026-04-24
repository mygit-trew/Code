"""
Walmart Price Scraper — Always, Tampax, Discreet
Uses Firecrawl for bot-proof browser rendering; saves results to walmart_prices.csv

Requires:
    pip install firecrawl-py
    export FIRECRAWL_API_KEY=your_key
"""

import csv
import json
import os
import random
import re
import time
from datetime import datetime
from urllib.parse import quote

from firecrawl import Firecrawl

# ── Config ────────────────────────────────────────────────────────────────────
BRANDS = ["Always", "Tampax", "Discreet"]
OUTPUT_FILE = "walmart_prices.csv"
MAX_PAGES_PER_BRAND = 30        # safety cap (~1,200 items/brand)
PAGE_DELAY = (4, 8)             # seconds between pages
BRAND_DELAY = (8, 12)           # seconds between brands
FIRECRAWL_WAIT_MS = 3000        # ms for JS to render before Firecrawl snapshots
MAX_RETRIES = 3                 # retries on transient Firecrawl errors

CSV_FIELDS = [
    "brand", "name", "item_id", "current_price", "was_price",
    "unit_price", "availability", "rating", "review_count", "url", "scraped_at",
]


# ── CSV helpers ────────────────────────────────────────────────────────────────

def init_csv(path: str) -> None:
    """Write header only if file doesn't already exist."""
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()


def append_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=CSV_FIELDS).writerows(rows)


# ── URL builder ────────────────────────────────────────────────────────────────

def build_search_url(brand: str, page: int) -> str:
    encoded = quote(brand)
    return f"https://www.walmart.com/search?q={encoded}&facet=brand%3A{encoded}&page={page}"


# ── HTML parsers ───────────────────────────────────────────────────────────────

def extract_next_data(html: str) -> dict | None:
    """Pull __NEXT_DATA__ JSON embedded in the page HTML."""
    m = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.+?)</script>',
        html, re.DOTALL,
    )
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            return None
    return None


def extract_via_html_fallback(html: str, brand: str, scraped_at: str) -> list[dict]:
    """Fallback: extract item IDs directly from raw HTML when __NEXT_DATA__ is absent."""
    products = []
    for m in re.finditer(r'data-item-id=["\'](\d+)["\']', html):
        item_id = m.group(1)
        products.append({
            "brand": brand, "name": "", "item_id": item_id,
            "current_price": "", "was_price": "", "unit_price": "",
            "availability": "", "rating": "", "review_count": "",
            "url": f"https://www.walmart.com/ip/{item_id}",
            "scraped_at": scraped_at,
        })
    # Deduplicate by item_id while preserving order
    seen: set[str] = set()
    unique = []
    for p in products:
        if p["item_id"] not in seen:
            seen.add(p["item_id"])
            unique.append(p)
    return unique


# ── Product record builder ─────────────────────────────────────────────────────

def parse_items(raw_items: list, brand: str, scraped_at: str) -> list[dict]:
    products = []
    for item in raw_items:
        price_info = item.get("priceInfo") or {}

        # Walmart's actual JSON uses flat strings, not nested dicts.
        # linePriceDisplay is the shelf price; itemPrice is set on some variants.
        current_price = (
            price_info.get("linePriceDisplay")
            or price_info.get("linePrice")
            or price_info.get("itemPrice")
            or ""
        )
        was_price  = price_info.get("wasPrice", "")
        unit_price = price_info.get("unitPrice", "")

        item_id   = str(item.get("itemId", ""))
        canonical = item.get("canonicalUrl", "")
        url = (
            f"https://www.walmart.com{canonical}" if canonical
            else (f"https://www.walmart.com/ip/{item_id}" if item_id else "")
        )
        if not item_id and url:
            m = re.search(r'/(\d+)\?', url)
            if m:
                item_id = m.group(1)

        products.append({
            "brand":         brand,
            "name":          item.get("name", ""),
            "item_id":       item_id,
            "current_price": current_price,
            "was_price":     was_price,
            "unit_price":    unit_price,
            "availability":  item.get("availabilityStatusDisplayValue", ""),
            "rating":        item.get("averageRating", ""),
            "review_count":  item.get("numberOfReviews", ""),
            "url":           url,
            "scraped_at":    scraped_at,
        })
    return products


# ── Firecrawl fetch with retry ─────────────────────────────────────────────────

def fetch_html(app: Firecrawl, url: str) -> str | None:
    """Fetch a page via Firecrawl, returning raw HTML.

    First attempt uses the default proxy. If Walmart serves a CAPTCHA page,
    retries automatically with proxy="stealth" (residential IPs). Falls back
    to exponential backoff on transient errors.
    """
    proxies = [None, "stealth"]  # escalate to residential on block
    for attempt in range(1, MAX_RETRIES + 1):
        proxy = proxies[min(attempt - 1, len(proxies) - 1)]
        kwargs = {"formats": ["raw_html"], "wait_for": FIRECRAWL_WAIT_MS}
        if proxy:
            kwargs["proxy"] = proxy
        try:
            result = app.scrape(url, **kwargs)
            html = (
                result.raw_html
                if hasattr(result, "raw_html")
                else (result or {}).get("raw_html")
            ) or ""
            if _is_blocked(html):
                print(f"      Blocked (attempt {attempt}/{MAX_RETRIES}), retrying with stealth proxy...")
                time.sleep(2)
                continue
            if html:
                return html
            print(f"      Empty response (attempt {attempt}/{MAX_RETRIES})")
        except Exception as e:
            print(f"      Firecrawl error (attempt {attempt}/{MAX_RETRIES}): {e}")
        if attempt < MAX_RETRIES:
            time.sleep(2 ** attempt)  # 2s, 4s backoff
    return None


def _is_blocked(html: str) -> bool:
    """Return True if Walmart served a bot-challenge page instead of search results."""
    title_m = re.search(r'<title>([^<]*)</title>', html, re.IGNORECASE)
    if not title_m:
        return False
    title = title_m.group(1).lower()
    return any(kw in title for kw in ("robot", "human", "captcha", "blocked", "denied"))


# ── Brand scraper ──────────────────────────────────────────────────────────────

def scrape_brand(app: Firecrawl, brand: str) -> int:
    """Scrape all search pages for one brand. Returns total products written."""
    total = 0
    scraped_at = datetime.now().isoformat(timespec="seconds")

    for page_num in range(1, MAX_PAGES_PER_BRAND + 1):
        url = build_search_url(brand, page_num)
        print(f"  [{brand}] page {page_num} — {url}")

        html = fetch_html(app, url)
        if not html:
            print(f"      Could not fetch page {page_num}, stopping brand.")
            break

        data = extract_next_data(html)
        items: list = []
        total_pages = page_num  # conservative default

        if data:
            try:
                search_result = (
                    data.get("props", {})
                        .get("pageProps", {})
                        .get("initialData", {})
                        .get("searchResult", {})
                )
                for stack in search_result.get("itemStacks", []):
                    items.extend(stack.get("items", []))
                pagination = search_result.get("paginationV2", {})
                total_pages = pagination.get("maxPage", page_num)
            except Exception as e:
                print(f"      JSON nav error: {e}")

        if not items:
            print("      No items in __NEXT_DATA__ — trying HTML fallback...")
            fallback = extract_via_html_fallback(html, brand, scraped_at)
            if not fallback:
                print("      HTML fallback also empty. Stopping brand.")
                break
            append_csv(OUTPUT_FILE, fallback)
            total += len(fallback)
            print(f"      +{len(fallback)} via fallback (brand total: {total})")
        else:
            parsed = parse_items(items, brand, scraped_at)
            append_csv(OUTPUT_FILE, parsed)
            total += len(parsed)
            print(f"      +{len(parsed)} products (brand total: {total}, pages: {page_num}/{total_pages})")

        if page_num >= total_pages:
            print(f"      Last page reached ({total_pages}).")
            break

        time.sleep(random.uniform(*PAGE_DELAY))

    return total


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        raise EnvironmentError("Set the FIRECRAWL_API_KEY environment variable before running.")

    print("\nWalmart Brand Price Scraper (Firecrawl)")
    print(f"Brands : {', '.join(BRANDS)}")
    print(f"Output : {OUTPUT_FILE}\n")

    init_csv(OUTPUT_FILE)
    app = Firecrawl(api_key=api_key)

    grand_total = 0
    for i, brand in enumerate(BRANDS):
        print(f"\n── Scraping brand: {brand} ──")
        count = scrape_brand(app, brand)
        grand_total += count
        print(f"   {brand} total: {count} products")

        if i < len(BRANDS) - 1:
            delay = random.uniform(*BRAND_DELAY)
            print(f"   Waiting {delay:.1f}s before next brand...")
            time.sleep(delay)

    print(f"\n✓ Done — {grand_total} total products saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
