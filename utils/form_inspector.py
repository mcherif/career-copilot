"""
Playwright form inspection utilities.

These helpers operate on an already-navigated Playwright Page object.
They are intentionally read-only: they detect and report fields but do
not fill or submit anything.
"""
from __future__ import annotations

from playwright.async_api import Page


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
    "button:has-text('Apply')",
    "a:has-text('Apply')",
    "button:has-text('Quick Apply')",
    "a:has-text('Quick Apply')",
]


async def try_click_apply(page: Page) -> bool:
    """Try to find and click an Apply button on the current page.

    Returns True if a button was found and clicked, False otherwise.
    After clicking, waits briefly for the page to settle.
    """
    for selector in _APPLY_SELECTORS:
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0 and await locator.is_visible(timeout=1500):
                await locator.click()
                try:
                    await page.wait_for_load_state("networkidle", timeout=8000)
                except Exception:
                    # networkidle may time out on SPAs; that is fine.
                    pass
                return True
        except Exception:
            continue
    return False


async def scan_fields(page: Page) -> list[dict]:
    """Return a list of visible form field descriptors from the current page.

    Each descriptor is a dict with keys:
        tag, type, name, id, placeholder, label, required
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
