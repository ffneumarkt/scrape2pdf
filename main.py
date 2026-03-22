import argparse
import asyncio
import os
import re
import sys
import unicodedata
from datetime import datetime

from playwright.async_api import async_playwright

BASE_URL = "https://www.ffneumarkt.at/2018/"
CONTENT_SELECTOR = ".content-wrapper > div:nth-child(1)"
REMOVE_SELECTORS = [
    ".nav-next",
    "div.slideshowlink",
]

DATE_PATTERNS = [
    # yyyy-MM-dd, yyyy/MM/dd, yyyy.MM.dd
    re.compile(r"\b(\d{4})[-/.](\d{2})[-/.](\d{2})\b"),
    # dd.MM.yyyy, dd/MM/yyyy
    re.compile(r"\b(\d{2})[./](\d{2})[./](\d{4})\b"),
]


def slugify_ascii(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.lower()
    ascii_text = re.sub(r"[^a-z0-9]+", "-", ascii_text)
    ascii_text = re.sub(r"-+", "-", ascii_text).strip("-")
    return ascii_text


def extract_date_and_title(h1_text: str):
    if not h1_text:
        return None, None

    for pattern in DATE_PATTERNS:
        match = pattern.search(h1_text)
        if not match:
            continue

        if pattern.pattern.startswith("\\b(\\d{4})"):
            y, m, d = match.group(1), match.group(2), match.group(3)
        else:
            d, m, y = match.group(1), match.group(2), match.group(3)

        try:
            dt = datetime.strptime(f"{y}-{m}-{d}", "%Y-%m-%d")
        except ValueError:
            continue

        date_str = dt.strftime("%Y%m%d")
        # Remove matched date substring from title
        title_wo_date = (h1_text[: match.start()] + " " + h1_text[match.end() :]).strip()
        title_wo_date = re.sub(r"\s+", " ", title_wo_date)
        return date_str, title_wo_date

    return None, None


def build_filename(date_str: str, title: str, post_id: int) -> str:
    slug = slugify_ascii(title)
    if slug:
        return f"{date_str}_{slug}_{post_id}.pdf"
    return f"{date_str}_{post_id}.pdf"


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Scrape ffneumarkt posts to PDF by ID range.")
    parser.add_argument("--start", type=int, required=True, help="Start post ID (inclusive)")
    parser.add_argument("--end", type=int, required=True, help="End post ID (inclusive)")
    parser.add_argument("--out-dir", required=True, help="Output directory for PDFs")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests in seconds")
    parser.add_argument("--concurrency", type=int, default=4, help="Number of concurrent requests")
    return parser.parse_args(argv)


async def process_post(post_id, args, browser, semaphore, counters, counters_lock):
    async with semaphore:
        url = f"{BASE_URL}?p={post_id}"
        try:
            page = await browser.new_page()
            response = await page.goto(url, wait_until="networkidle", timeout=60000)
            status = response.status if response else None

            if status == 404:
                print(f"[skip] {post_id}: 404")
                async with counters_lock:
                    counters["skipped"] += 1
                await page.close()
                return

            h1 = page.locator("h1").first
            if not h1 or await h1.count() == 0:
                print(f"[skip] {post_id}: no <h1>")
                async with counters_lock:
                    counters["skipped"] += 1
                await page.close()
                return

            h1_text = (await h1.inner_text()).strip()
            date_str, title_wo_date = extract_date_and_title(h1_text)
            if not date_str:
                print(f"[skip] {post_id}: no valid date in <h1>")
                async with counters_lock:
                    counters["skipped"] += 1
                await page.close()
                return

            has_content = await page.evaluate(
                """(payload) => {
                    const selector = payload.selector;
                    const removeSelectors = payload.removeSelectors || [];
                    const el = document.querySelector(selector);
                    if (!el) return false;
                    const clone = el.cloneNode(true);
                    clone.querySelectorAll("a[href]").forEach((a) => {
                        a.removeAttribute("href");
                    });
                    removeSelectors.forEach((sel) => {
                        clone.querySelectorAll(sel).forEach((n) => {
                            n.remove();
                        });
                    });
                    document.body.innerHTML = "";
                    document.body.appendChild(clone);
                    return true;
                }""",
                {"selector": CONTENT_SELECTOR, "removeSelectors": REMOVE_SELECTORS},
            )
            if not has_content:
                print(f"[skip] {post_id}: content selector not found")
                async with counters_lock:
                    counters["skipped"] += 1
                await page.close()
                return

            filename = build_filename(date_str, title_wo_date, post_id)
            out_path = os.path.join(args.out_dir, filename)

            await page.pdf(
                path=out_path,
                print_background=True,
                display_header_footer=False,
                margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
            )
            print(f"[save] {post_id}: {filename}")
            async with counters_lock:
                counters["saved"] += 1
        except Exception as exc:
            print(f"[error] {post_id}: {exc}", file=sys.stderr)
            async with counters_lock:
                counters["errored"] += 1
        finally:
            try:
                await page.close()
            except Exception:
                pass
            await asyncio.sleep(args.delay)


async def main_async(argv):
    args = parse_args(argv)

    if args.start > args.end:
        print("Error: --start must be <= --end", file=sys.stderr)
        return 2

    os.makedirs(args.out_dir, exist_ok=True)

    counters = {"saved": 0, "skipped": 0, "errored": 0}
    counters_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(max(1, args.concurrency))

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        tasks = [
            process_post(post_id, args, browser, semaphore, counters, counters_lock)
            for post_id in range(args.start, args.end + 1)
        ]
        await asyncio.gather(*tasks)
        await browser.close()

    print(
        f"Done. saved={counters['saved']} skipped={counters['skipped']} errored={counters['errored']}"
    )
    return 0


def main(argv):
    return asyncio.run(main_async(argv))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
