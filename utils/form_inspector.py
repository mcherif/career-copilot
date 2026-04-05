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
    "realworkfromanywhere.com",
    "jobicy.com",
    "euremotejobs.com",
    "dynamitejobs.com",
    "jobspresso.co",
    "workingnomads.com",
    "arcdev.app",
    "dailyremote.com",
    "himalayas.app",
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
            if await locator.count() == 0 or not await locator.is_visible(timeout=300):
                continue

            # Watch for a new tab opened by target="_blank" links.
            # Timeout capped at 5s — if no new tab opens in that time the
            # click navigated the current tab instead (handled below).
            async with context.expect_page(timeout=5000) as new_page_info:
                await locator.click(timeout=5000)
            new_page = await new_page_info.value
            await new_page.wait_for_load_state("load", timeout=20000)
            return True, new_page

        except Exception:
            # expect_page() raises if no new tab was opened within its timeout.
            # Fall through and check whether the current page navigated instead.
            try:
                locator2 = page.locator(selector).first
                if await locator2.count() > 0 and await locator2.is_visible(timeout=500):
                    await locator2.click(timeout=5000)
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

    Implemented as a single JS evaluation to avoid the latency of
    individual per-element async calls (previously ~7 round-trips per field).
    """
    try:
        fields: list[dict] = await page.evaluate("""() => {
            const sel = [
                "input:not([type='hidden']):not([type='submit'])" +
                    ":not([type='button']):not([type='reset']):not([type='image'])",
                "textarea",
                "select"
            ].join(",");

            function isVisible(el) {
                const r = el.getBoundingClientRect();
                if (r.width === 0 && r.height === 0) return false;
                const s = window.getComputedStyle(el);
                if (s.display === 'none' || s.visibility === 'hidden') return false;
                // Radio/checkbox inputs are often opacity:0 in styled forms
                // (the visual circle is a CSS overlay).  Allow them through —
                // fill_form uses check(force=True) which bypasses opacity.
                const t = el.type || '';
                if (t === 'radio' || t === 'checkbox') return true;
                return s.opacity !== '0';
            }

            function getLabel(el, id) {
                // 1. <label for="id">
                if (id) {
                    const lb = document.querySelector('label[for="' + id + '"]');
                    if (lb) return lb.innerText.trim();
                }
                // 2. aria-label
                const al = el.getAttribute('aria-label');
                if (al && al.trim()) return al.trim();
                // 3. aria-labelledby
                const alb = el.getAttribute('aria-labelledby');
                if (alb) {
                    const txt = alb.split(' ')
                        .map(lid => document.getElementById(lid))
                        .filter(Boolean)
                        .map(e => e.innerText.trim())
                        .filter(Boolean)
                        .join(' ');
                    if (txt) return txt;
                }
                // 4. Walk up parent elements to find a <label for="parentId"> —
                //    Ashby puts the for= on a wrapper div, not on the <input>.
                let node = el.parentElement;
                for (let d = 0; d < 8; d++) {
                    if (!node) break;
                    if (node.id) {
                        const lb = document.querySelector('label[for="' + node.id + '"]');
                        if (lb) return lb.innerText.trim();
                    }
                    // Sibling <label> immediately preceding this container
                    let sib = node.previousElementSibling;
                    while (sib) {
                        if (sib.tagName === 'LABEL') return sib.innerText.trim();
                        const inner = sib.querySelector('label');
                        if (inner) return inner.innerText.trim();
                        sib = sib.previousElementSibling;
                    }
                    node = node.parentElement;
                }
                return '';
            }

            const fields = Array.from(document.querySelectorAll(sel))
                .filter(isVisible)
                .map(el => {
                    const tag = el.tagName.toLowerCase();
                    const id  = el.id || '';
                    const name = el.name || '';
                    const placeholder = el.placeholder || '';
                    const label = getLabel(el, id);
                    if (!name && !id && !placeholder && !label) return null;
                    return {
                        tag,
                        type: el.type || tag,
                        name,
                        id,
                        placeholder,
                        label,
                        required: el.required || false,
                        role: el.getAttribute('role') || '',
                    };
                })
                .filter(Boolean);

            // Pass 2: non-input comboboxes — Ashby EEO dropdowns and similar
            // ATSes that render dropdowns as <button aria-haspopup="listbox">
            // or <div role="combobox">.  These are invisible to the input/select
            // selector above but are fully interactive.
            const capturedIds = new Set(fields.map(f => f.id).filter(Boolean));
            const sel2 = [
                "button[aria-haspopup='listbox']",
                "[role='combobox']:not(input):not(select)",
                "[aria-haspopup='listbox']:not(input):not(select)",
            ].join(',');
            const fields2 = Array.from(document.querySelectorAll(sel2))
                .filter(el => {
                    if (!isVisible(el)) return false;
                    if (el.id && capturedIds.has(el.id)) return false;
                    return true;
                })
                .map(el => {
                    const tag = el.tagName.toLowerCase();
                    const id  = el.id || '';
                    const label = getLabel(el, id);
                    if (!id && !label) return null;
                    return {
                        tag,
                        type: 'text',
                        name: el.getAttribute('name') || '',
                        id,
                        placeholder: '',
                        label,
                        required: el.getAttribute('aria-required') === 'true',
                        role: 'combobox',
                    };
                })
                .filter(Boolean);

            return [...fields, ...fields2];
        }""")
        return fields or []
    except Exception:
        return []


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
