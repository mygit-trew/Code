"""
Walmart Price Scraper — Always, Tampax, Discreet
Uses Playwright with the system's Chrome browser (headless=False).

If Walmart's bot-protection (PerimeterX) appears, the Chrome window is kept open so
the user can solve the press-and-hold CAPTCHA once.  After that, the session is valid
for the rest of the run and the scraper continues automatically.

Requires:
    pip install playwright
    playwright install chromium          # only needed as fallback
    Google Chrome: /Applications/Google Chrome.app   (macOS default)
"""

import csv
import os
import random
import re
import time
from datetime import datetime
from urllib.parse import quote

from playwright.sync_api import Page, sync_playwright

# ── Config ────────────────────────────────────────────────────────────────────
BRANDS = ["Always", "Tampax", "Discreet"]
OUTPUT_FILE = "walmart_prices.csv"
MAX_PAGES_PER_BRAND = 30        # safety cap
PAGE_DELAY = (4, 8)             # seconds between pages
BRAND_DELAY = (8, 12)           # seconds between brands
CAPTCHA_TIMEOUT_MS = 120_000    # ms the user has to solve the CAPTCHA (2 minutes)

CSV_FIELDS = [
    "brand", "name", "item_id", "current_price", "was_price",
    "unit_price", "availability", "rating", "review_count", "url", "scraped_at",
]

# JS run inside the browser to scrape each product tile
_EXTRACT_JS = """
() => {
    return Array.from(document.querySelectorAll('div[data-item-id]')).map(el => {
        const titleEl = el.querySelector('[data-automation-id="product-title"]');
        const priceEl = el.querySelector('[data-automation-id="product-price"]');
        const ratingEl = el.querySelector('[aria-label*=" stars"]') ||
                         el.querySelector('[aria-label*="out of 5"]');
        const reviewEl = el.querySelector('[aria-label*="review"]');
        const availEl  = el.querySelector('[data-automation-id="fulfillment-badge"]') ||
                         el.querySelector('[class*="availability"]');
        return {
            item_id:      el.getAttribute('data-item-id'),
            name:         titleEl   ? titleEl.textContent.trim()                         : '',
            price_text:   priceEl   ? priceEl.textContent.trim()                         : '',
            rating_label: ratingEl  ? (ratingEl.getAttribute('aria-label') || '')        : '',
            review_label: reviewEl  ? (reviewEl.getAttribute('aria-label') ||
                                       reviewEl.textContent.trim())                       : '',
            availability: availEl   ? availEl.textContent.trim()                         : '',
        };
    });
}
"""


# ── CSV helpers ────────────────────────────────────────────────────────────────

def init_csv(path: str) -> None:
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()


def append_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=CSV_FIELDS).writerows(rows)


# ── URL builder ────────────────────────────────────────────────────────────────

def build_search_url(brand: str, page_num: int) -> str:
    encoded = quote(brand)
    return f"https://www.walmart.com/search?q={encoded}&facet=brand%3A{encoded}&page={page_num}"


# ── Price parser ──────────────────────────────────────────────────────────────

def parse_price(text: str) -> tuple[str, str, str]:
    """Return (current_price, was_price, unit_price) from Walmart's price block text.

    Walmart embeds price as e.g. '$1457current price $14.57was $18.9844.9 ¢/count'.
    'current price $X.XX' is the canonical shelf price; 'was $X.XX' is the strikethrough.
    """
    current_m = re.search(r'current price \$(\d+\.?\d*)', text, re.IGNORECASE)
    if not current_m:
        # Fallback: first lone dollar amount (no adjacent digits immediately before $)
        current_m = re.search(r'(?<!\d)\$(\d+\.\d{2})', text)
    current = ('$' + current_m.group(1)) if current_m else ''

    was_m = re.search(r'was\s+\$(\d+\.?\d*)', text, re.IGNORECASE)
    was = ('$' + was_m.group(1)) if was_m else ''

    # e.g. "44.9 ¢/count" or "$0.45/oz"
    unit_m = re.search(r'\d+\.?\d*\s*[¢$]\s*/\s*\w+', text)
    unit = unit_m.group(0).strip() if unit_m else ''

    return current, was, unit


# ── Item record builder ────────────────────────────────────────────────────────

def build_records(raw_items: list, brand: str, scraped_at: str) -> list[dict]:
    records = []
    for item in raw_items:
        item_id = str(item.get("item_id") or "")
        current_price, was_price, unit_price = parse_price(item.get("price_text", ""))

        rating_label = item.get("rating_label", "")
        rating_m = re.search(r'(\d+\.?\d*)\s*(?:out of|stars)', rating_label, re.IGNORECASE)
        rating = rating_m.group(1) if rating_m else ""

        review_label = item.get("review_label", "")
        review_m = re.search(r'(\d[\d,]*)', review_label)
        review_count = review_m.group(1).replace(",", "") if review_m else ""

        records.append({
            "brand":         brand,
            "name":          item.get("name", ""),
            "item_id":       item_id,
            "current_price": current_price,
            "was_price":     was_price,
            "unit_price":    unit_price,
            "availability":  item.get("availability", ""),
            "rating":        rating,
            "review_count":  review_count,
            "url":           f"https://www.walmart.com/ip/{item_id}" if item_id else "",
            "scraped_at":    scraped_at,
        })
    return records


# ── CAPTCHA handler ───────────────────────────────────────────────────────────

def wait_if_blocked(pw_page: Page, context_label: str) -> bool:
    """If the page is on /blocked, pause and wait for the user to solve the CAPTCHA.

    Returns True once unblocked, False if timeout exceeded.
    """
    if "/blocked" not in pw_page.url:
        return True
    print(f"\n{'='*60}")
    print(f"  CAPTCHA required for: {context_label}")
    print(f"  Please look at the Chrome window and press-and-hold")
    print(f"  the 'Tap and hold' button until it completes.")
    print(f"  You have {CAPTCHA_TIMEOUT_MS // 1000} seconds.")
    print(f"{'='*60}\n")
    try:
        pw_page.wait_for_url(lambda u: "/blocked" not in u, timeout=CAPTCHA_TIMEOUT_MS)
        print("  CAPTCHA solved — continuing.\n")
        pw_page.wait_for_timeout(2000)  # brief settle
        return True
    except Exception:
        print("  CAPTCHA not solved in time. Stopping brand.\n")
        return False


# ── Page fetcher ──────────────────────────────────────────────────────────────

def fetch_page_items(pw_page: Page, url: str) -> list | None:
    """Navigate to url, handle any CAPTCHA, and return raw DOM item dicts."""
    for attempt in range(1, 4):
        try:
            pw_page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            pw_page.wait_for_timeout(3000 + random.randint(0, 2000))
        except Exception as e:
            print(f"      Navigation error (attempt {attempt}/3): {e}")
            if attempt < 3:
                time.sleep(2 ** attempt)
            continue

        if "/blocked" in pw_page.url:
            if not wait_if_blocked(pw_page, url):
                return None
            pw_page.wait_for_timeout(3000)

        try:
            raw = pw_page.evaluate(_EXTRACT_JS)
            return raw
        except Exception as e:
            print(f"      JS extraction error (attempt {attempt}/3): {e}")
        if attempt < 3:
            time.sleep(2 ** attempt)
    return None


# ── Brand scraper ──────────────────────────────────────────────────────────────

def scrape_brand(pw_page: Page, brand: str) -> int:
    total = 0
    scraped_at = datetime.now().isoformat(timespec="seconds")

    for page_num in range(1, MAX_PAGES_PER_BRAND + 1):
        url = build_search_url(brand, page_num)
        print(f"  [{brand}] page {page_num} — {url}")

        raw = fetch_page_items(pw_page, url)
        if raw is None:
            print(f"      Could not fetch page {page_num}, stopping brand.")
            break
        if not raw:
            print(f"      No items on page {page_num} — last page reached.")
            break

        records = build_records(raw, brand, scraped_at)
        append_csv(OUTPUT_FILE, records)
        total += len(records)
        print(f"      +{len(records)} products (brand total: {total})")

        time.sleep(random.uniform(*PAGE_DELAY))

    return total


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    print("\nWalmart Brand Price Scraper (Chrome / CAPTCHA-assisted)")
    print(f"Brands : {', '.join(BRANDS)}")
    print(f"Output : {OUTPUT_FILE}")
    print("Note   : A Chrome window will open. If a CAPTCHA appears,")
    print("         press-and-hold the button in the browser to unblock.\n")

    init_csv(OUTPUT_FILE)
    grand_total = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=False,
            channel="chrome",
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(locale="en-US")
        pw_page = context.new_page()

        # Warm-up: visit homepage to establish cookies
        try:
            pw_page.goto("https://www.walmart.com", wait_until="domcontentloaded", timeout=35_000)
            pw_page.wait_for_timeout(random.randint(2000, 4000))
            try:
                pw_page.click('button[aria-label*="close" i]', timeout=3000)
                pw_page.wait_for_timeout(800)
            except Exception:
                pass
            pw_page.keyboard.press("Escape")
            pw_page.wait_for_timeout(500)
        except Exception as e:
            print(f"Warm-up failed (non-fatal): {e}")

        for i, brand in enumerate(BRANDS):
            print(f"\n── Scraping brand: {brand} ──")
            count = scrape_brand(pw_page, brand)
            grand_total += count
            print(f"   {brand} total: {count} products")

            if i < len(BRANDS) - 1:
                delay = random.uniform(*BRAND_DELAY)
                print(f"   Waiting {delay:.1f}s before next brand...")
                time.sleep(delay)

        context.close()
        browser.close()

    print(f"\n✓ Done — {grand_total} total products saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
