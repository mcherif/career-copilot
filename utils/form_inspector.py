"""
Playwright form inspection utilities.

These helpers operate on an already-navigated Playwright Page object.
They are intentionally read-only: they detect and report fields but do
not fill or submit anything.
"""
from __future__ import annotations

from playwright.async_api import Page, BrowserContext


# Ordered by specificity — first match wins.
_APPLY_SELECTORS = [
    "[data-qa='apply-button']",
    "#apply-button",
    ".apply-button",
    "a.apply",
    "button:has-text('Apply Now')",
    "a:has-text('Apply Now')",
    "button:has-text('Apply for this Job')",
    "a:has-text('Apply for this Job')",
    "button:has-text('Apply for this position')",
    "a:has-text('Apply for this position')",
    "button:has-text('Apply')",
    "a:has-text('Apply')",
    "button:has-text('Quick Apply')",
    "a:has-text('Quick Apply')",
]

# Text patterns for extracting the direct employer apply URL from listing pages
# (e.g. Remotive, We Work Remotely) without clicking.
_APPLY_LINK_TEXTS = [
    "apply for this position",
    "apply for this job",
    "apply now",
    "apply here",
    "original job posting",
    "apply",
]

# Domains that are job listing aggregators, not employer pages.
_LISTING_DOMAINS = [
    "remotive.com",
    "weworkremotely.com",
    "remoteok.com",
    "linkedin.com",
    "indeed.com",
    "glassdoor.com",
    "levels.fyi",
]


async def extract_apply_url(page: Page) -> str | None:
    """Extract the direct employer apply URL from a job listing page.

    Checks all links against each phrase in priority order so that
    high-confidence phrases (e.g. "apply for this position") always win
    over lower-confidence ones (e.g. "original job posting").
    Ignores links that stay on the same listing platform.
    Returns the first external match, or None if nothing is found.
    """
    current_domain = _domain_of(page.url)

    links: list[dict] = await page.evaluate(
        """() => Array.from(document.querySelectorAll("a[href]")).map(el => ({
            text: el.innerText.trim().toLowerCase(),
            href: el.href
        }))"""
    )

    external = [
        link for link in links
        if link["href"]
        and link["href"].startswith("http")
        and _domain_of(link["href"]) != current_domain
    ]

    # Iterate phrases in priority order; return the first link that matches.
    for phrase in _APPLY_LINK_TEXTS:
        for link in external:
            if phrase in link["text"]:
                return link["href"]

    return None


async def try_click_apply(page: Page) -> tuple[bool, Page]:
    """Try to find and click an Apply button on the current page.

    Returns (clicked, active_page).  If clicking opens a new browser tab,
    active_page is that new tab (so callers can continue working on it).
    Otherwise active_page is the same page that was passed in.

    After clicking, waits briefly for the page to settle.
    """
    context: BrowserContext = page.context

    for selector in _APPLY_SELECTORS:
        try:
            locator = page.locator(selector).first
            if await locator.count() == 0 or not await locator.is_visible(timeout=1500):
                continue

            # Watch for a new tab opened by target="_blank" links.
            async with context.expect_page() as new_page_info:
                await locator.click()
            new_page = await new_page_info.value
            await new_page.wait_for_load_state("load", timeout=20000)
            return True, new_page

        except Exception:
            # expect_page() raises if no new tab was opened within its timeout.
            # Fall through and check whether the current page navigated instead.
            try:
                locator2 = page.locator(selector).first
                if await locator2.count() > 0 and await locator2.is_visible(timeout=500):
                    await locator2.click()
                    try:
                        await page.wait_for_load_state("networkidle", timeout=8000)
                    except Exception:
                        pass
                    return True, page
            except Exception:
                continue

    return False, page


async def scan_fields(page: Page) -> list[dict]:
    """Return a list of visible form field descriptors from the current page.

    Each descriptor is a dict with keys:
        tag, type, name, id, placeholder, label, required

    Filters out Notion's internal editor inputs by ignoring fields that
    carry no identifying information (name, id, placeholder, aria-label,
    or a linked <label>).
    """
    fields: list[dict] = []

    selectors = [
        "input:not([type='hidden']):not([type='submit']):not([type='button'])"
        ":not([type='reset']):not([type='image'])",
        "textarea",
        "select",
    ]

    for selector in selectors:
        elements = await page.query_selector_all(selector)
        for el in elements:
            try:
                if not await el.is_visible():
                    continue

                tag: str = await el.evaluate("el => el.tagName.toLowerCase()")
                field_type: str = await el.get_attribute("type") or tag
                name: str = await el.get_attribute("name") or ""
                field_id: str = await el.get_attribute("id") or ""
                placeholder: str = await el.get_attribute("placeholder") or ""
                required: bool = await el.evaluate("el => el.required") or False
                label_text = ""

                if field_id:
                    try:
                        label = await page.query_selector(f"label[for='{field_id}']")
                        if label:
                            label_text = (await label.inner_text()).strip()
                    except Exception:
                        pass

                # Fall back to aria-label when no <label> element is linked.
                if not label_text:
                    label_text = await el.get_attribute("aria-label") or ""

                # Skip fully anonymous inputs (e.g. Notion editor divs rendered
                # as contenteditable that also emit input events).
                if not any([name, field_id, placeholder, label_text]):
                    continue

                fields.append(
                    {
                        "tag": tag,
                        "type": field_type,
                        "name": name,
                        "id": field_id,
                        "placeholder": placeholder,
                        "label": label_text,
                        "required": required,
                    }
                )
            except Exception:
                continue

    return fields


def format_field_report(fields: list[dict]) -> str:
    """Return a human-readable summary of detected form fields."""
    if not fields:
        return "  (no form fields detected)"

    lines = []
    for f in fields:
        display = f["label"] or f["placeholder"] or f["name"] or f["id"] or "(unlabeled)"
        req = " *" if f["required"] else ""
        lines.append(
            f"  [{f['type']}]{req} {display}"
            f"  (name={f['name'] or '-'}, id={f['id'] or '-'})"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _domain_of(url: str) -> str:
    """Return the netloc (host) portion of a URL, or empty string on failure."""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.lower()
    except Exception:
        return ""
