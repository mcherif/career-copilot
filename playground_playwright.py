import argparse
import asyncio
import sqlite3
import sys
from typing import Optional, Tuple


def _get_shortlisted_job(db_path: str) -> Optional[Tuple[str, str, str]]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        row = cur.execute(
            """
            SELECT title, company, url
            FROM jobs
            WHERE status = 'shortlisted' AND url IS NOT NULL AND url != ''
            ORDER BY fit_score DESC, id ASC
            LIMIT 1
            """
        ).fetchone()
        return row
    finally:
        conn.close()


async def _run_browser(target_url: str, seconds: int, headless: bool) -> None:
    from playwright.async_api import async_playwright

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=headless)
        page = await browser.new_page()
        await page.goto(target_url, wait_until="load", timeout=30000)
        await page.wait_for_timeout(max(1, seconds) * 1000)
        await browser.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Minimal Playwright playground: open a shortlisted job URL, wait, and close."
    )
    parser.add_argument("--db", default="career_copilot.db", help="Path to SQLite database")
    parser.add_argument("--url", help="Explicit URL to open instead of the top shortlisted job")
    parser.add_argument("--seconds", type=int, default=5, help="Seconds to keep the page open")
    parser.add_argument(
        "--headless",
        dest="headless",
        action="store_true",
        help="Run headless for environment validation",
    )
    parser.add_argument(
        "--headful",
        dest="headless",
        action="store_false",
        help="Run with a visible browser window",
    )
    parser.set_defaults(headless=False)
    args = parser.parse_args()

    target_url = args.url
    selected_job = None

    if not target_url:
        selected_job = _get_shortlisted_job(args.db)
        if not selected_job:
            print("No shortlisted job with a URL was found in the database.", file=sys.stderr)
            return 1
        title, company, target_url = selected_job
        print(f"Opening shortlisted job: {title} -- {company}")

    print(f"URL: {target_url}")
    print(f"Headless: {args.headless}")
    print(f"Waiting for {args.seconds} seconds...")

    try:
        asyncio.run(_run_browser(target_url, args.seconds, args.headless))
    except ImportError as exc:
        print(
            "Playwright import failed. The current Python environment appears to have a broken "
            f"Playwright/greenlet installation: {exc}",
            file=sys.stderr,
        )
        print(
            "Try reinstalling Playwright and greenlet in the active environment, then rerun this script.",
            file=sys.stderr,
        )
        return 1

    print("Playwright playground run completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
