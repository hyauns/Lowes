"""
Phase 5.0 — block detection for Lowes scraper.

Two distinct block types behave very differently on lowes.com:

1. Cloudflare interactive challenge
   - User-solvable (CAPTCHA, JS challenge, "Just a moment")
   - DOM has `#challenge-form` or `iframe[src*="challenges.cloudflare.com"]`
   - Title typically "Just a moment..." or "Attention Required"
   - Worker must PAUSE (hold the claim, wait for human via UI button).

2. Akamai server-side Access Denied
   - NOT user-solvable in-place — page is a static error
   - Signature text "Access Denied", "you don't have permission to access",
     "errors.edgesuite.net", "Reference #"
   - Worker must take recovery action (Google detour, profile swap, etc.)

`detect_block(page)` returns a small dict; callers dispatch on `.kind`.
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # avoid forcing playwright import on test runners
    from playwright.async_api import Page


# Substrings (case-insensitive) that mark each block kind.
# Tested against page title + visible body text.
_CF_TITLE_PATTERNS = (
    "just a moment",
    "attention required",
    "checking your browser",
)
_CF_BODY_PATTERNS = (
    "checking your browser",
    "challenge platform",
    "cf-challenge",
)
_AD_BODY_PATTERNS = (
    "access denied",
    "you don't have permission to access",
    "errors.edgesuite.net",
)
# Title-based AD signatures. Some Akamai/Lowes interstitials set the
# <title> to "Access Denied" while body innerText is sparse or hidden, so
# the body-text branch misses them. Observed in product 5005553847 (2026-05-17)
# which got saved with title="Access Denied", brand="Access", price="$649.00".
_AD_TITLE_PATTERNS = (
    "access denied",
    "you don't have permission",
)
# Akamai's reference number format, e.g. "Reference #18.b1d02e17.1778950810.5ae4ab89"
_AD_REF_RE = re.compile(r"reference\s*#\s*[0-9a-f.]+", re.IGNORECASE)


async def _safe_eval(page: "Page", expr: str, default=None):
    try:
        return await page.evaluate(expr)
    except Exception:
        return default


async def detect_block(page: "Page") -> dict:
    """Inspect the current page and classify any block in place.

    Returns a dict:
      {
        "kind": "cloudflare" | "access_denied" | "ok",
        "detail": str,                # short human-readable reason
        "signals": list[str],         # which signals fired (for logs)
      }

    Cheap — designed to be called once per detail-scrape after navigation,
    before extraction. Never raises (catches all Playwright errors).
    """
    signals: list[str] = []

    # 1) Cloudflare-specific DOM markers — most decisive.
    cf_dom = await _safe_eval(
        page,
        """
        () => {
            const sels = [
                '#challenge-form',
                'iframe[src*="challenges.cloudflare.com"]',
                'div.cf-browser-verification',
                'div#cf-please-wait',
                'div[class*="cf-challenge"]',
            ];
            for (const s of sels) {
                try { if (document.querySelector(s)) return s; } catch (_) {}
            }
            return null;
        }
        """,
    )
    if cf_dom:
        signals.append(f"cf-dom:{cf_dom}")
        return {
            "kind": "cloudflare",
            "detail": f"Cloudflare challenge element present ({cf_dom})",
            "signals": signals,
        }

    # 2) Title-based detection — quick to read.
    title = (await _safe_eval(page, "() => document.title || ''", "") or "").lower()
    for p in _CF_TITLE_PATTERNS:
        if p in title:
            signals.append(f"cf-title:{p}")
            return {
                "kind": "cloudflare",
                "detail": f"Cloudflare title pattern: '{p}' in '{title[:80]}'",
                "signals": signals,
            }
    # Akamai Access Denied via title — catches the case where the body
    # text doesn't contain the AD signature (cached/hybrid pages).
    for p in _AD_TITLE_PATTERNS:
        if p in title:
            signals.append(f"ad-title:{p}")
            return {
                "kind": "access_denied",
                "detail": f"Akamai title pattern: '{p}' in '{title[:80]}'",
                "signals": signals,
            }

    # 3) Body text — read a bounded prefix to keep it cheap.
    #    Akamai's Access Denied page is short, signal will be in first 4 KB.
    body = await _safe_eval(
        page,
        "() => (document.body ? document.body.innerText.slice(0, 4000) : '')",
        "",
    ) or ""
    body_low = body.lower()

    for p in _AD_BODY_PATTERNS:
        if p in body_low:
            signals.append(f"ad-body:{p}")
            # Pull the Akamai reference number if present — useful for support.
            ref_match = _AD_REF_RE.search(body)
            ref = ref_match.group(0) if ref_match else None
            detail = f"Akamai Access Denied ('{p}')"
            if ref:
                detail += f" — {ref}"
                signals.append(f"ad-ref:{ref}")
            return {
                "kind": "access_denied",
                "detail": detail,
                "signals": signals,
            }

    for p in _CF_BODY_PATTERNS:
        if p in body_low:
            signals.append(f"cf-body:{p}")
            return {
                "kind": "cloudflare",
                "detail": f"Cloudflare body pattern: '{p}'",
                "signals": signals,
            }

    # 4) Lone reference-number pattern as a last resort (Akamai sometimes
    #    serves bare "Reference #X" with minimal surrounding text).
    if _AD_REF_RE.search(body):
        ref = _AD_REF_RE.search(body).group(0)
        signals.append(f"ad-ref-only:{ref}")
        return {
            "kind": "access_denied",
            "detail": f"Akamai Reference present without standard message: {ref}",
            "signals": signals,
        }

    return {"kind": "ok", "detail": "", "signals": []}


# ─── Result-dict block detection (Phase 5.5) ─────────────────────────────

def result_looks_like_block(result: dict | None) -> dict | None:
    """Inspect a SCRAPED detail dict for block-page indicators.

    Complementary to `detect_block(page)`: by the time the worker checks
    the live page, it may have moved on. The scraped result itself sometimes
    still carries the AD overlay's title/brand even though body extraction
    pulled real product fields from __NEXT_DATA__. We catch those cases here.

    Returns a verdict dict in the same shape as `detect_block`
    ({"kind", "detail", "signals"}), or None if the result looks clean.
    """
    if not isinstance(result, dict):
        return None
    title = (result.get("title") or "").strip().lower()
    if not title:
        return None
    for p in _AD_TITLE_PATTERNS:
        if p in title:
            return {
                "kind": "access_denied",
                "detail": f"scraped title matches AD pattern '{p}': {result.get('title')!r}",
                "signals": [f"result-title:{p}"],
            }
    return None
