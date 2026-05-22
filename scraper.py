"""
Lowes.com Category Scraper
Uses AdsPower + Playwright to scrape product listings and details.

Usage:
    python scraper.py discover                                          - Find all categories
    python scraper.py setzip                                            - Set zip code to 10001
    python scraper.py list <url>                                        - Scrape all pages
    python scraper.py list <url> --pages 1-2                            - Scrape page 1 to 2 only
    python scraper.py list <url> --pages 1                              - Scrape page 1 only
    python scraper.py detail <url>                                      - Scrape product details
    python scraper.py full <url>                                        - Full pipeline
    python scraper.py full <url> --pages 1-3                            - Full pipeline, pages 1-3
"""
import asyncio
import json
import os
import random
import re
import sys
import time
from datetime import datetime
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from adspower_helper import AdsPower
from config import (
    ADSPOWER_API, ADSPOWER_PROFILE_ID, BASE_URL, ZIP_CODE,
    MIN_DELAY, MAX_DELAY, CATEGORY_DELAY, TIMEOUT,
    DATA_DIR, LISTINGS_DIR, DETAILS_DIR, DEBUG_DIR,
)
from state_manager import StateManager


def _safe_name(name):
    return "".join(c if c.isalnum() or c in " -_" else "_" for c in name).strip()


def _name_from_url(url):
    """Extract the most specific category name from a Lowes /pl/ URL.

    Handles flat AND nested subcategory paths:
      /pl/power-tools/4294607842                          -> power-tools
      /pl/power-tools/power-tool-combo-kits/4294607831    -> power-tool-combo-kits
      /pl/foo/bar/baz/1234567                             -> baz

    Strategy: take all segments after `/pl/`, drop any trailing numeric
    category-id segments, and return the last remaining one. Numeric segments
    are Lowes' opaque taxonomy IDs and don't make good filenames.
    """
    path = urlparse(url).path if url.startswith("http") else url
    m = re.search(r"/pl/(.+)", path)
    if not m:
        # No /pl/ in path — fall back to last non-empty path segment.
        last = path.rstrip("/").split("/")[-1]
        return last or "unknown"
    segs = [s for s in m.group(1).split("/") if s]
    while segs and segs[-1].isdigit():
        segs.pop()
    return segs[-1] if segs else "unknown"


def _parse_pages(args):
    """Parse --pages flag from args. Returns (page_from, page_to) or (1, None) for all."""
    for i, arg in enumerate(args):
        if arg == "--pages" and i + 1 < len(args):
            val = args[i + 1]
            if "-" in val:
                parts = val.split("-", 1)
                return int(parts[0]), int(parts[1])
            else:
                return int(val), int(val)
    return 1, None  # None = scrape all pages


def _save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


# Phase 5.5: detail files may live under one of two filenames:
#   data/details/<pid>.json            — normal (has images)
#   data/details/<pid>_missimage.json  — same data but images=[] (user must add)
# These helpers centralise the lookup so callers don't have to check both.

def detail_paths(pid: str) -> tuple[str, str]:
    """Return (standard_path, missimage_path) for a product id."""
    pid = str(pid)
    std = os.path.join(DETAILS_DIR, f"{pid}.json")
    miss = os.path.join(DETAILS_DIR, f"{pid}_missimage.json")
    return std, miss


def detail_path_existing(pid: str) -> str | None:
    """Return whichever filename actually exists on disk, or None."""
    std, miss = detail_paths(pid)
    if os.path.exists(std):
        return std
    if os.path.exists(miss):
        return miss
    return None


def load_detail(pid: str) -> dict | None:
    """Load a product's detail JSON from whichever filename variant exists."""
    p = detail_path_existing(pid)
    return _load_json(p) if p else None


async def _delay(lo=None, hi=None):
    await asyncio.sleep(random.uniform(lo or MIN_DELAY, hi or MAX_DELAY))


# ─── Proxy-dead detection (Phase 5.3) ───────────────────────────────────────

class ProxyDeadError(RuntimeError):
    """Raised by _goto when consecutive navigations fail with proxy-level errors.

    Caller (Worker) catches this and runs `switch_to_local_network` recovery,
    instead of letting the scraper grind through empty pages and rack up
    `partial` outcomes (the bug seen in earlier logs).
    """


class BrowserClosedError(RuntimeError):
    """Raised when an operation hits a Playwright "Target page/context/browser
    has been closed" error. Means the underlying browser is gone (AdsPower
    stopped the profile, CDP disconnected, or recovery closed the scraper).
    Worker should reconnect, NOT continue grinding through accordion clicks
    that all fail with the same error (the spam-log bug from 2026-05-17).
    """


class PageCrashedError(RuntimeError):
    """Raised when a page operation hits 'Target crashed' / 'Page crashed'.

    The Chrome renderer for this tab died but the browser context is still
    alive. Recovery is cheap: close the dead page and open a new one in the
    same context — no AdsPower restart, no CDP reconnect.

    Without this, the error fell through the generic Exception handler in
    worker._process_job; the worker marked the item failed and immediately
    tried the next one, which crashed the same way — burning through the
    whole queue in seconds and leaving other workers with nothing to claim
    (the "3 workers shrink to 1" symptom).
    """


# Chrome network error codes that mean "the proxy itself didn't respond /
# refused / dropped the connection" — distinct from server-side block pages.
_PROXY_ERROR_PATTERNS = (
    "ERR_CONNECTION_CLOSED",
    "ERR_PROXY_CONNECTION_FAILED",
    "ERR_TUNNEL_CONNECTION_FAILED",
    "ERR_SOCKS_CONNECTION_FAILED",
    "ERR_CONNECTION_REFUSED",
    "ERR_CONNECTION_RESET",
)

# Substrings that indicate the Playwright page/context/browser is gone.
# Different Playwright versions phrase this slightly differently — match all.
_BROWSER_CLOSED_PATTERNS = (
    "Target page, context or browser has been closed",
    "Target closed",
    "Browser has been closed",
    "Connection closed",
    "websocket.send while connection is",
)

# Substrings that mean the renderer (tab) crashed but the browser is still up.
# Order: check page-crash BEFORE browser-closed so we pick the cheap recovery.
_PAGE_CRASHED_PATTERNS = (
    "Target crashed",
    "Page crashed",
    "Page.evaluate: Target crashed",
)

# After this many consecutive proxy errors in a row, treat the proxy as dead.
# Single ERR_CONNECTION_CLOSED can be a transient blip; two in a row on
# different URLs is a strong signal the proxy has actually died.
PROXY_DEAD_THRESHOLD = 2


def _is_proxy_error(exc) -> bool:
    s = str(exc)
    return any(p in s for p in _PROXY_ERROR_PATTERNS)


def _is_browser_closed_error(exc) -> bool:
    s = str(exc)
    return any(p in s for p in _BROWSER_CLOSED_PATTERNS)


def _is_page_crashed_error(exc) -> bool:
    s = str(exc)
    return any(p in s for p in _PAGE_CRASHED_PATTERNS)


# ─── Scraper ────────────────────────────────────────────────────────────────

class LowesScraper:
    def __init__(self, profile_id=None, worker_id="w0", state: "StateManager | None" = None):
        self.ads = AdsPower(ADSPOWER_API, profile_id or ADSPOWER_PROFILE_ID)
        self.pw = None
        self.browser = None
        self.page = None
        self.worker_id = worker_id
        self.state = state  # optional; legacy commands work without it
        # Cooperative stop signal — UI sets this asyncio.Event between items.
        self._ui_stop = None
        # Phase 5a: set by warmup() — used as Referer for subsequent /pd/ visits
        # so Lowes sees navigation from a category page, not a direct hit.
        self._referer = None
        self._warmed_up = False
        # Phase 5.3: consecutive-proxy-error counter. Incremented in _goto on
        # ERR_CONNECTION_CLOSED / ERR_PROXY_CONNECTION_FAILED etc. Reset on
        # any successful navigation or non-proxy exception. When it crosses
        # PROXY_DEAD_THRESHOLD, _goto raises ProxyDeadError so Worker can
        # trigger switch_to_local_network recovery.
        self._proxy_error_count = 0
        # Set True by page.on("crash") so workers can poll for crash even when
        # the current Playwright operation hasn't raised yet.
        self._page_crashed = False

    async def connect(self):
        # ads.start() is a blocking HTTP call — run in a thread so we don't
        # stall the event loop (which would freeze the FastAPI server).
        #
        # CDP-connect retry loop (2026-05-20): on slow / VPS hosts, the
        # first connect_over_cdp can hang for minutes because the proxy-
        # bound Chrome process takes a while to spin up devtools target
        # listing. Original code used Playwright's default 180s timeout
        # and gave up on first failure → whole worker pool died if even
        # one profile was slow. New behaviour: short per-attempt timeout,
        # bounded retry, idempotent ads.start so a manually-opened
        # profile keeps its session. Knobs (config.py):
        #   • CDP_CONNECT_MAX_ATTEMPTS    (default 40)
        #   • CDP_CONNECT_RETRY_DELAY_SECONDS (default 15)
        #   • CDP_CONNECT_TIMEOUT_MS      (default 30000)
        try:
            import config as _cfg
            max_attempts = int(getattr(_cfg, "CDP_CONNECT_MAX_ATTEMPTS", 40))
            retry_delay = int(getattr(_cfg, "CDP_CONNECT_RETRY_DELAY_SECONDS", 15))
            per_attempt_ms = int(getattr(_cfg, "CDP_CONNECT_TIMEOUT_MS", 30000))
        except Exception:
            max_attempts, retry_delay, per_attempt_ms = 40, 15, 30000

        self.pw = await async_playwright().start()
        last_err = None
        for attempt in range(1, max_attempts + 1):
            if self._ui_stop is not None and self._ui_stop.is_set():
                raise RuntimeError(
                    f"[{self.worker_id}] connect aborted by stop request"
                )
            try:
                ws = await asyncio.to_thread(self.ads.start)
                print(
                    f"[{self.worker_id}] [Connect] attempt {attempt}/{max_attempts}: "
                    f"CDP connect (timeout {per_attempt_ms / 1000:.0f}s)..."
                )
                self.browser = await self.pw.chromium.connect_over_cdp(
                    ws, timeout=per_attempt_ms
                )
                break
            except Exception as e:
                last_err = e
                msg = f"{type(e).__name__}: {str(e)[:240]}"
                if attempt < max_attempts:
                    print(
                        f"[{self.worker_id}] [Connect] attempt {attempt} FAILED: "
                        f"{msg}. Retrying in {retry_delay}s..."
                    )
                    # Sleep in 1s slices so a stop request is honoured promptly.
                    for _ in range(retry_delay):
                        if self._ui_stop is not None and self._ui_stop.is_set():
                            raise RuntimeError(
                                f"[{self.worker_id}] connect aborted by stop request"
                            )
                        await asyncio.sleep(1)
                else:
                    raise RuntimeError(
                        f"[{self.worker_id}] AdsPower CDP connect failed after "
                        f"{max_attempts} attempts. Last error: {msg}"
                    ) from last_err

        ctx = self.browser.contexts[0]
        # Prefer the newest non-closed page (avoids latching onto a tab that
        # crashed before we reconnected — e.g. _reconnect_after_browser_closed
        # after a renderer crash). Falls back to a fresh page if none usable.
        chosen = None
        try:
            for p in reversed(ctx.pages):
                if not p.is_closed():
                    chosen = p
                    break
        except Exception:
            chosen = None
        self.page = chosen or await ctx.new_page()
        self._install_page_safety_handlers(self.page)
        print(f"[{self.worker_id}] [Scraper] Connected to AdsPower browser")

    def _install_page_safety_handlers(self, page) -> None:
        """Wire renderer-crash + JS-dialog listeners on a page.

        Why this exists:
          • page.on("crash") gives us a proactive log when Chrome's renderer
            for this tab dies. Without it, we only learn via the next
            Playwright op (which may hang silently if it has no timeout).
          • page.on("dialog") auto-dismisses any alert/confirm/prompt that
            Lowes might pop up. Playwright's default behavior is to wait
            for a handler — without one, every subsequent op hangs forever,
            which matches the "browser open but worker stuck" symptom on
            VPS.
        Idempotent-ish: handlers reference `self`, so reattaching after
        recover_page is fine; old page is closed so its listeners die with it.
        """
        wid = self.worker_id

        def _on_crash(_p):
            try:
                self._page_crashed = True
                print(f"  [{wid}] [Scraper] page.on('crash') fired — renderer died")
            except Exception:
                pass

        async def _on_dialog(d):
            try:
                msg = (d.message or "")[:200]
                print(f"  [{wid}] [Scraper] auto-dismiss dialog ({d.type}): {msg}")
                await d.dismiss()
            except Exception:
                pass

        try:
            page.on("crash", _on_crash)
            page.on("dialog", lambda d: asyncio.create_task(_on_dialog(d)))
        except Exception as e:
            print(f"  [{wid}] [Scraper] could not install page handlers: {e}")

    async def close(self):
        if self.pw:
            await self.pw.stop()
        print("[Scraper] Disconnected")

    async def recover_page(self) -> bool:
        """Replace the current page after a 'Target/Page crashed' error.

        Cheap recovery — doesn't touch AdsPower, doesn't reconnect CDP.
        The browser context is still alive; only the renderer for this tab
        died. Steps:
          1. Best-effort close the dead page (so AdsPower doesn't accumulate
             zombie tabs visible to the user).
          2. Prefer a non-closed page already in the context (covers the case
             where the user pressed F5 / Ctrl+T in the AdsPower window
             manually — the new tab is picked up automatically).
          3. Otherwise open a fresh page in the same context.
          4. Reset warmup state — next /pd/ visit will warm up again.

        Returns True if we ended up with a usable page, False if the browser
        context itself is also gone (caller should escalate to a full
        reconnect).
        """
        try:
            if not self.browser or not self.browser.contexts:
                return False
            ctx = self.browser.contexts[0]
        except Exception:
            return False

        # Wrap close + new_page in wait_for so a wedged Chrome can't hang
        # recovery indefinitely. 10s each is generous; if it blows past that,
        # caller escalates to a full browser reconnect.
        try:
            if self.page and not self.page.is_closed():
                await asyncio.wait_for(self.page.close(), timeout=10)
        except Exception:
            pass  # dead page may not even close cleanly

        candidate = None
        try:
            for p in reversed(ctx.pages):
                if not p.is_closed():
                    candidate = p
                    break
        except Exception:
            candidate = None

        if candidate is None:
            try:
                candidate = await asyncio.wait_for(ctx.new_page(), timeout=10)
            except Exception as e:
                print(
                    f"  [{self.worker_id}] [Scraper] recover_page: "
                    f"new_page() failed: {e}"
                )
                return False

        self.page = candidate
        self._warmed_up = False
        self._proxy_error_count = 0
        self._page_crashed = False
        self._install_page_safety_handlers(self.page)
        print(f"  [{self.worker_id}] [Scraper] page replaced after crash")
        return True

    # ── Helpers ──────────────────────────────────────────────────────────

    async def _goto(self, url, referer=None):
        full = url if url.startswith("http") else f"{BASE_URL}{url}"
        # Use explicit referer if provided, else fall back to last warm-up category.
        # Playwright passes this as the HTTP Referer header — Lowes sees this
        # visit as navigation from a real page, not a fresh direct hit.
        ref = referer or self._referer
        nav_ok = False
        try:
            kwargs = {"wait_until": "domcontentloaded", "timeout": TIMEOUT}
            if ref:
                kwargs["referer"] = ref
            await self.page.goto(full, **kwargs)
            nav_ok = True
        except Exception as e:
            if _is_browser_closed_error(e):
                # Underlying browser is gone — propagate so worker reconnects
                # instead of continuing to grind page operations that all fail
                # the same way (the spam-log bug).
                raise BrowserClosedError(
                    f"browser closed during navigation: {e}"
                ) from e
            if _is_page_crashed_error(e):
                # Tab crashed — worker recovers cheaply by replacing the page.
                raise PageCrashedError(
                    f"page crashed during navigation: {e}"
                ) from e
            if _is_proxy_error(e):
                # Proxy-level failure — bump counter, surface as typed error
                # once we've seen enough in a row to be confident.
                self._proxy_error_count += 1
                print(
                    f"  [{self.worker_id}] [Proxy] error "
                    f"{self._proxy_error_count}/{PROXY_DEAD_THRESHOLD}: {e}"
                )
                if self._proxy_error_count >= PROXY_DEAD_THRESHOLD:
                    raise ProxyDeadError(
                        f"{self._proxy_error_count} consecutive proxy errors; last: {e}"
                    ) from e
            else:
                # Non-proxy failure (slow page, timeout etc.) — count as a clean
                # break in the streak.
                print(f"  [Warn] Navigation slow, continuing anyway: {e}")
                self._proxy_error_count = 0
        if nav_ok:
            self._proxy_error_count = 0
        # Wait for page content to actually render
        try:
            await self.page.wait_for_selector("body", timeout=10000)
        except Exception as e:
            # Surface browser-closed early; ignore other timeouts as before.
            if _is_browser_closed_error(e):
                raise BrowserClosedError(
                    f"browser closed waiting for body: {e}"
                ) from e
            if _is_page_crashed_error(e):
                raise PageCrashedError(
                    f"page crashed waiting for body: {e}"
                ) from e
        await _delay(3, 6)
        return full

    async def _first_product_id_on_page(self):
        """Return the productId of the first product card visible on the page,
        or None if no /pd/ link is in the DOM yet.

        Used to detect SPA state changes between listing pages: after navigating
        to page N+1, the first card's productId should differ from page N's.
        If it doesn't change within a timeout, React hasn't swapped the data yet.
        """
        return await self.page.evaluate(r"""
            () => {
                const a = document.querySelector('a[href*="/pd/"]');
                if (!a) return null;
                const m = a.getAttribute('href').match(/\/pd\/[^/]+\/(\d+)/);
                return m ? m[1] : null;
            }
        """)

    async def _scroll_to_bottom(self):
        """Scroll the page in chunks so Lowes' lazy-load (IntersectionObserver-
        based) renders product cards AND the pagination footer.

        Lowes only injects the bottom pagination row once the user has scrolled
        near enough — without this, `<a aria-label="arrow right">` may be
        absent from DOM or `visibility: hidden`. Confirmed via user-supplied
        HTML + intermittent Playwright `wait_for(visible)` timeouts.
        """
        try:
            await self.page.evaluate("""
                async () => {
                    const step = Math.max(400, window.innerHeight * 0.8);
                    let last = -1;
                    let pos = 0;
                    // Loop until the page height stops growing (lazy content
                    // appends as we scroll). Bound to ~25 iterations as a safety.
                    for (let i = 0; i < 25; i++) {
                        const max = document.body.scrollHeight;
                        if (max === last && pos >= max) break;
                        last = max;
                        window.scrollTo(0, Math.min(pos, max));
                        await new Promise(r => setTimeout(r, 250));
                        pos += step;
                        if (pos >= max + step) {
                            // Reached the current bottom; settle and re-check.
                            window.scrollTo(0, document.body.scrollHeight);
                            await new Promise(r => setTimeout(r, 400));
                            if (document.body.scrollHeight === max) break;
                        }
                    }
                    window.scrollTo(0, document.body.scrollHeight);
                }
            """)
            # Brief settle so newly-injected pagination DOM is ready.
            await asyncio.sleep(1.0)
        except Exception as e:
            print(f"  [Pagination] scroll error (continuing): {e}")

    async def _click_next_listing(self) -> bool:
        """Click the Lowes 'arrow right' Next link to advance the listing.

        Why click instead of page.goto(next_href)?  Lowes' /pl/ pages are an
        SPA: pagination is client-side. Navigating directly to
        '/pl/.../?offset=22' makes the server return offset=0 content, then
        the React app's initial-load XHR also fetches offset=0 — so every
        direct navigation lands on page 1 (confirmed in user logs: page 2/3/4
        all returned identical first-pids to page 1). Clicking the Next link
        triggers the SPA's own route change which DOES respect offset.

        Returns True if click fired, False if Next link is missing/disabled
        (end of pagination, caller should stop).
        """
        # 1. Scroll first so Lowes lazy-renders the pagination footer.
        await self._scroll_to_bottom()

        # 2. Try the visible-element path via locator (handles clicks naturally).
        try:
            loc = self.page.locator(
                'a[aria-label="arrow right"], a.arrow[href*="offset"]'
            ).first
            await loc.wait_for(state="visible", timeout=8000)
            if await loc.get_attribute("aria-disabled") == "true":
                print("  [Pagination] Next link is aria-disabled → end of pagination")
                return False
            href = await loc.get_attribute("href")
            if not href or href == "#":
                print("  [Pagination] Next link has no usable href → end of pagination")
                return False
            print(f"  [Pagination] clicking Next (href={href})")
            await loc.scroll_into_view_if_needed()
            await loc.click()
            return True
        except Exception as e:
            # 3. Fallback: maybe the link is in DOM but Playwright considers it
            #    not-visible (offscreen / CSS quirk). Probe via JS and, if a
            #    valid href exists, dispatch the click directly via JS.
            probe = await self.page.evaluate("""
                () => {
                    const a = document.querySelector('a[aria-label="arrow right"], a.arrow[href*="offset"]');
                    if (!a) return null;
                    return {
                        href: a.getAttribute('href') || '',
                        disabled: a.getAttribute('aria-disabled') === 'true',
                        in_dom: true,
                    };
                }
            """)
            if not probe:
                print(f"  [Pagination] Next link not in DOM after scroll: {e}")
                return False
            if probe.get('disabled') or not probe.get('href') or probe['href'] == '#':
                print(f"  [Pagination] Next link present but disabled/no-href → end of pagination")
                return False
            print(
                f"  [Pagination] locator-click failed ({type(e).__name__}); "
                f"falling back to JS click. href={probe['href']}"
            )
            try:
                await self.page.evaluate("""
                    () => {
                        const a = document.querySelector('a[aria-label="arrow right"], a.arrow[href*="offset"]');
                        if (a) { a.scrollIntoView({block:'center'}); a.click(); }
                    }
                """)
                return True
            except Exception as je:
                print(f"  [Pagination] JS-click also failed: {je}")
                return False

    async def _wait_for_listing_render(self, previous_first_pid: str | None, timeout_ms: int = 15000):
        """Block until the SPA shows products NEW vs the previous page.

        Returns True if a new first-product-card was detected within timeout,
        False on timeout (DOM stuck — caller should treat as end-of-category /
        soft block and stop paginating).

        If no previous_first_pid is given (first page of the run), we just wait
        for ANY product card to appear so extraction doesn't race the initial
        XHR. Also best-effort waits for the pagination Next link so the probe
        below is reliable.
        """
        try:
            if previous_first_pid:
                # Scope to #listItems specifically. Lowes' SSR HTML often
                # leaves stale page-1 /pd/ links elsewhere in the document
                # (Recently Viewed sidebar, sponsored carousel, cached
                # SSR fragments) — checking document-wide returned the old
                # pid for the full 15s and falsely concluded "end of
                # category". The product grid the extractor actually reads
                # is `#listItems`; React clears it then re-populates via
                # XHR when offset= changes, so that's the authoritative
                # signal that pagination has advanced.
                await self.page.wait_for_function(
                    r"""prev => {
                        const scope = document.querySelector('#listItems');
                        if (!scope) return false;
                        const a = scope.querySelector('a[href*="/pd/"]');
                        if (!a) return false;
                        const m = a.getAttribute('href').match(/\/pd\/[^/]+\/(\d+)/);
                        return !!(m && m[1] !== prev);
                    }""",
                    arg=previous_first_pid,
                    timeout=timeout_ms,
                )
            else:
                # First page of this run — wait for #listItems to have
                # any product link (falls back to document-wide if Lowes
                # hasn't rendered #listItems for this category yet).
                try:
                    await self.page.wait_for_function(
                        r"""() => {
                            const scope = document.querySelector('#listItems');
                            return !!(scope && scope.querySelector('a[href*="/pd/"]'));
                        }""",
                        timeout=timeout_ms,
                    )
                except Exception:
                    await self.page.wait_for_selector('a[href*="/pd/"]', timeout=3000)
        except Exception:
            return False

        # Now that products are rendered, give the Next-page anchor a moment to
        # hydrate too so the pagination probe doesn't return 'missing' for a
        # page that actually has more.
        try:
            await self.page.wait_for_selector(
                'a[aria-label="arrow right"], a.arrow[href*="offset"]',
                timeout=4000,
            )
        except Exception:
            # No pagination link → could legitimately be the last page; let
            # the probe + empty-page logic decide.
            pass
        return True

    async def _next_data(self):
        """Extract __NEXT_DATA__ JSON from current page."""
        try:
            return await self.page.evaluate("""
                () => {
                    const el = document.getElementById('__NEXT_DATA__');
                    return el ? JSON.parse(el.textContent) : null;
                }
            """)
        except Exception as e:
            print(f"  [Warn] __NEXT_DATA__ extraction failed: {e}")
            return None

    async def _save_debug(self, label, data):
        os.makedirs(DEBUG_DIR, exist_ok=True)
        path = os.path.join(DEBUG_DIR, f"{label}_{int(time.time())}.json")
        _save_json(path, data)
        print(f"  [Debug] Saved to {path}")

    # ── Set Zip Code ─────────────────────────────────────────────────────

    async def set_zip(self):
        """Set Lowes delivery zip via header tooltip flow.

        Flow (current Lowes UI):
          1. Header shows  [truck-icon] <link data-testid='update-zipcode-link'>NNNNN</link>
          2. Click the link → tooltip popup appears with input + Update button
          3. Type zip → "Update" button enables → click → page updates
        """
        print(f"[Scraper] Setting zip code to {ZIP_CODE}...")
        await self._goto(BASE_URL)

        try:
            zip_link = self.page.locator('[data-testid="update-zipcode-link"]').first
            await zip_link.wait_for(state="visible", timeout=15000)

            # Skip if already correct
            current = (await zip_link.text_content() or "").strip()
            if current == ZIP_CODE:
                print(f"[Scraper] Zip already set to {ZIP_CODE}, skipping")
                return

            print(f"  [Zip] Current: '{current}' → changing to {ZIP_CODE}")
            await zip_link.scroll_into_view_if_needed()
            await zip_link.click()
            await _delay(1, 2)

            # The tooltip input — class .searchBoxContainer is specific to this widget.
            zip_input = self.page.locator(
                'input.searchBoxContainer, '
                'input[placeholder="Enter Zip Code"], '
                '.deliveryzipcodewrapper input[type="text"]'
            ).first
            await zip_input.wait_for(state="visible", timeout=8000)
            await zip_input.click()
            await zip_input.fill("")
            # Type slowly so the React form-state listener registers each keystroke
            # and the Update button transitions from disabled → enabled.
            await zip_input.type(ZIP_CODE, delay=80)
            await _delay(0.5, 1.2)

            # Update button is .updateButton inside the popup.
            # Wait for it to become enabled (it loses the [disabled] attr).
            update_btn = self.page.locator(
                '.deliveryzipcodewrapper button.updateButton:not([disabled]), '
                'button.updateButton:not([disabled])'
            ).first
            try:
                await update_btn.wait_for(state="visible", timeout=5000)
            except Exception:
                # Fallback: any visible Update button in the popup
                update_btn = self.page.locator(
                    '.deliveryzipcodewrapper button:has-text("Update")'
                ).first
                await update_btn.wait_for(state="visible", timeout=3000)

            await update_btn.click()
            await _delay(2, 4)

            # Verify: the link text should now be the new zip
            try:
                new_text = (await zip_link.text_content() or "").strip()
                if new_text == ZIP_CODE:
                    print(f"[Scraper] Zip code set to {ZIP_CODE}")
                else:
                    print(f"[Scraper] Zip applied but link shows '{new_text}' (may still be updating)")
            except Exception:
                print(f"[Scraper] Zip code submitted ({ZIP_CODE})")
        except Exception as e:
            print(f"[Warn] Auto zip-set error: {e}")
            # If running under UI, don't block on input() — user can see the browser
            # via AdsPower and continue manually.
            if self._ui_stop is not None:
                print("  [Zip] Running under UI: open AdsPower window, set zip manually, then proceed.")
                await _delay(3, 5)
                return
            print("  Please set zip manually, then press Enter...")
            await asyncio.get_event_loop().run_in_executor(None, input)

    # ── Warm-up (Phase 5a) ───────────────────────────────────────────────

    async def warmup(self, category_url):
        """Build a realistic browsing session before hitting /pd/ links directly.

        Why: Lowes/Akamai soft-blocks bots that hit product pages cold (no
        Referer, no session cookies). Symptom is a 'rendered' page whose
        accordions don't open and __NEXT_DATA__ is incomplete → our scraper
        flags everything missing and loops on needs_refill.

        Flow:
          1. Visit homepage (no referer)            → seeds Akamai/CF cookies
          2. set_zip (idempotent)                   → unlocks pricing/availability
          3. Visit category_url with Referer=home   → looks like nav click
          4. Try clicking a product card on category page (best effort)
          5. Stash category_url as default referer for subsequent /pd/ visits

        Returns True on success, False on any failure. Never raises — caller
        should still let the worker pick up jobs even if warmup didn't fully
        land (cap-attempts will catch persistent block).
        """
        try:
            print(f"[{self.worker_id}] [Warmup] step 1/4: homepage")
            self._referer = None  # ensure no stale referer for first hit
            await self._goto(BASE_URL)
            await _delay(2, 4)

            print(f"[{self.worker_id}] [Warmup] step 2/4: zip")
            try:
                await self.set_zip()
            except Exception as e:
                print(f"[{self.worker_id}] [Warmup] set_zip failed (continuing): {e}")

            print(f"[{self.worker_id}] [Warmup] step 3/4: category {category_url}")
            await self._goto(category_url, referer=BASE_URL + "/")
            await _delay(3, 5)

            print(f"[{self.worker_id}] [Warmup] step 4/4: click a product card")
            try:
                # Lowes product cards on category pages use these patterns.
                # We try several selectors and stop at the first that yields
                # a visible element — Lowes A/B-tests the layout.
                card = self.page.locator(
                    'a[data-testid="productCard-imageLink"], '
                    'a[data-testid="product-card-link"], '
                    'a.product-card-link, '
                    'a[href*="/pd/"]'
                ).first
                await card.wait_for(state="visible", timeout=8000)
                # We don't actually wait for product page to fully load — just
                # need the click to register as an outgoing nav from category.
                async with self.page.expect_navigation(
                    wait_until="domcontentloaded", timeout=15000
                ):
                    await card.click()
                await _delay(2, 4)
                print(f"[{self.worker_id}] [Warmup] product card clicked OK")
            except Exception as e:
                print(f"[{self.worker_id}] [Warmup] product click skipped: {e}")

            # Stash the category URL as our standing Referer. Every subsequent
            # _goto() without explicit referer will use this.
            self._referer = category_url
            self._warmed_up = True
            print(f"[{self.worker_id}] [Warmup] done. Referer set to category.")
            return True
        except Exception as e:
            print(f"[{self.worker_id}] [Warmup] FAILED (worker still active): {e}")
            self._warmed_up = False
            return False

    # ── Discover Categories ──────────────────────────────────────────────

    async def discover(self):
        """Navigate to shop-all-departments and extract category links."""
        print("[Scraper] Discovering categories...")
        await self._goto("/l/shop-all-departments")

        cats = await self.page.evaluate("""
            () => {
                const links = document.querySelectorAll('a[href*="/pl/"]');
                const cats = [], seen = new Set();
                for (const a of links) {
                    const href = a.getAttribute('href');
                    const text = a.textContent.trim();
                    if (href && text && !seen.has(href)) {
                        seen.add(href);
                        cats.push({name: text, url: href});
                    }
                }
                return cats;
            }
        """)

        # Also save raw __NEXT_DATA__ for analysis
        nd = await self._next_data()

        result = {
            "scraped_at": datetime.now().isoformat(),
            "total_categories": len(cats),
            "categories": cats,
        }
        _save_json(os.path.join(DATA_DIR, "categories.json"), result)

        if nd:
            await self._save_debug("categories_next_data", nd)

        print(f"[Scraper] Found {len(cats)} categories:")
        for i, c in enumerate(cats):
            print(f"  [{i:3d}] {c['name']:<40s} {c['url']}")

        return cats

    # ── Scrape Category Listing ──────────────────────────────────────────

    async def _listing_handle_block(self, block: dict, page_num: int) -> bool:
        """Phase 5.6: listing-side AD recovery (mirrors Worker.rotate_and_recreate).

        Reusing the same AdsPower profile after rotation still gets blocked
        because Akamai fingerprints the profile, not just the IP. So the full
        recovery is: rotate IP via API → wait alive → DELETE old profile →
        CREATE fresh profile bound to (rotated) proxy → reconnect.

        Returns True on success (caller retries the same page), False otherwise.
        Listing has only ONE dedicated proxy, so there's no swap-to-alive-slot
        tier; if recovery fails the listing scrape stops with whatever it's
        collected so far.
        """
        kind = block.get("kind", "?")
        detail = block.get("detail", "")
        print(f"[Listing] BLOCK on page {page_num}: kind={kind} | {detail}")

        # Lazy-load config so /api/config/reload mid-run picks up edits
        from importlib import reload as _reload
        import config as _cfg
        _reload(_cfg)
        listing_proxy = (getattr(_cfg, "LISTING_PROXY", "") or "").strip()
        listing_key = (getattr(_cfg, "LISTING_ROTATION_KEY", "") or "").strip()
        api_url = _cfg.ADSPOWER_API

        if not listing_key:
            print("[Listing] No LISTING_ROTATION_KEY configured — cannot rotate. Stopping.")
            return False
        if not listing_proxy:
            print("[Listing] LISTING_PROXY is empty — rotation would have no effect. Stopping.")
            return False

        old_pid = self.ads.pid
        print(f"[Listing] burning old profile {old_pid}, rotating IP, creating new profile")

        # 1) Close browser + stop AdsPower profile
        try:
            await self.close()
        except Exception as e:
            print(f"[Listing] close error during recovery (continuing): {e}")
        try:
            await asyncio.to_thread(self.ads.stop)
        except Exception as e:
            print(f"[Listing] AdsPower stop error during recovery (continuing): {e}")
        await asyncio.sleep(1.5)

        # 2) Rotate egress IP — blocks until proxy actually alive (max 30 min).
        from proxy_rotator import rotate_proxy_ip
        rot = await asyncio.to_thread(
            rotate_proxy_ip,
            listing_key,
            listing_proxy,
            log_prefix="[Listing][rotate]",
        )
        if not rot.get("ok"):
            print(f"[Listing] rotation failed: {rot.get('message')} — stopping.")
            return False
        new_ip = rot.get("new_ip") or rot.get("old_ip")

        # 3) DELETE the burned profile — Akamai has fingerprinted it.
        from adspower_helper import delete_profile, create_profile, find_group_id, list_groups
        try:
            await asyncio.to_thread(delete_profile, api_url, old_pid)
            print(f"[Listing] deleted old profile {old_pid}")
        except Exception as e:
            print(f"[Listing] DELETE old profile failed: {e} — stopping recovery.")
            return False
        await asyncio.sleep(1.0)

        # 4) CREATE a fresh profile bound to the (rotated) proxy.
        from proxy_parser import parse_proxy, to_adspower_user_proxy_config
        try:
            proxy_obj = parse_proxy(listing_proxy)
            proxy_cfg = to_adspower_user_proxy_config(proxy_obj)
            gid = await asyncio.to_thread(find_group_id, api_url, "Scraper")
            if not gid:
                groups = await asyncio.to_thread(list_groups, api_url)
                gid = str(groups[0]["group_id"]) if groups else None
            name = f"lowes-listing-rotated-{int(time.time())}"
            new_pid = await asyncio.to_thread(
                create_profile,
                api_url,
                name=name,
                proxy_config=proxy_cfg,
                group_id=gid,
                os_type="windows",   # listing only — fingerprint diversity not critical
            )
            print(
                f"[Listing] created new profile {new_pid} on LISTING_PROXY "
                f"(rotated IP={new_ip})"
            )
            print(
                f"[Listing] >>> If you want to persist this profile across runs, "
                f"update LISTING_PROFILE_ID in config.py to {new_pid!r} <<<"
            )
        except Exception as e:
            print(f"[Listing] create_profile failed: {e} — stopping recovery.")
            return False

        # 5) Re-bind the scraper to the new profile and reconnect.
        self.ads.pid = new_pid
        try:
            await self.connect()
        except Exception as e:
            print(f"[Listing] reconnect after recreate failed: {e} — stopping.")
            return False

        # 6) Force re-warmup so subsequent navigations look like fresh user flow.
        self._warmed_up = False
        self._proxy_error_count = 0
        print(f"[Listing] recovery complete — retrying page {page_num} with new IP + fresh profile")
        return True

    async def scrape_listing(self, category_url, page_from=1, page_to=None):
        """Scrape product listings from a category with pagination.

        Args:
            category_url: Full URL like https://www.lowes.com/pl/power-tools/4294607842
            page_from: Start page (default 1)
            page_to: End page (default None = all pages)
        """
        category_name = _name_from_url(category_url)
        pages_label = f"pages {page_from}-{page_to}" if page_to else f"page {page_from}+"

        print(f"\n{'='*60}")
        print(f"[Listing] {category_name} ({pages_label})")
        print(f"{'='*60}")

        # Phase 4.2 warm-up: scrape_listing used to vào thẳng /pl/<cat> không có
        # session cookies → Lowes có thể serve trang trống. set_zip() đã đi qua
        # homepage rồi, dùng nó như warm-up cheap. Idempotent qua _warmed_up flag.
        if not self._warmed_up:
            print(f"[Listing] Warming up via set_zip (homepage visit)...")
            try:
                await self.set_zip()
            except Exception as e:
                print(f"  [Warn] set_zip failed (continuing): {e}")
            self._referer = BASE_URL + "/"
            self._warmed_up = True

        out_file = os.path.join(LISTINGS_DIR, f"{_safe_name(category_name)}.json")

        # Resume support
        all_products = []
        seen_ids = set()
        existing = _load_json(out_file)
        if existing:
            all_products = existing.get("products", [])
            seen_ids = {str(p.get("productId", "")) for p in all_products}
            print(f"  [Resume] {len(all_products)} existing products loaded "
                  f"({len(seen_ids)} unique productIds)")

        base = category_url if category_url.startswith("http") else f"{BASE_URL}{category_url}"

        # Pagination URL normalisation (2026-05-18).
        # Two issues surfaced together:
        #   1. Users may paste a URL that ALREADY contains `offset=N` (e.g.
        #      they grabbed the URL from page 2 in the browser). The old
        #      builder just appended `&offset=24` for page 2, producing two
        #      conflicting `offset=` params → Lowes serves
        #      "Looks Like This Page Is Missing or Moved".
        #   2. For URLs with a `rangeFacet=` segment, Lowes only honours
        #      `offset=` when it appears BEFORE `rangeFacet=` in the query
        #      string; appending at the end is silently dropped.
        # Strategy: parse any existing offset out of the user URL once, use it
        # as `start_offset`, and for each page rebuild the URL with offset
        # placed before `rangeFacet=` if present (otherwise appended).
        _m = re.search(r"[?&]offset=(\d+)", base)
        start_offset = int(_m.group(1)) if _m else 0
        clean_base = re.sub(r"[?&]offset=\d+", "", base)
        clean_base = re.sub(r"\?&", "?", clean_base)
        clean_base = re.sub(r"&&+", "&", clean_base)
        clean_base = re.sub(r"[?&]$", "", clean_base)
        if start_offset:
            print(
                f"  [URL] base already contains offset={start_offset} — "
                f"treating that as page_from anchor; subsequent pages step +24"
            )

        def _build_page_url(target_offset: int) -> str:
            if target_offset == 0:
                return clean_base
            if "rangeFacet=" in clean_base:
                return re.sub(
                    r"([?&])rangeFacet=",
                    rf"\1offset={target_offset}&rangeFacet=",
                    clean_base,
                    count=1,
                )
            sep = "&" if "?" in clean_base else "?"
            return f"{clean_base}{sep}offset={target_offset}"

        # SPA detection: productId of the first card on the previous page.
        # After clicking Next, we wait until the DOM's first /pd/ link uses a
        # NEW productId. Without this, the React state-swap is async and
        # extraction can race the old DOM.
        prev_first_pid: str | None = None

        consecutive_empty = 0
        page_num = page_from
        while True:
            if page_to and page_num > page_to:
                print(f"  [Done] Reached target page {page_to}")
                break
            if self._ui_stop is not None and self._ui_stop.is_set():
                print(f"  [Listing] Stop requested — exiting at page {page_num}")
                break

            # Pagination via direct URL with offset placed correctly (see
            # _build_page_url docstring above).
            target_offset = start_offset + (page_num - page_from) * 24
            page_url = _build_page_url(target_offset)
            print(f"\n  [Page {page_num}] {page_url}")
            await self._goto(page_url)

            # Wait for the SPA to actually render NEW products before extracting.
            # On page_from this just waits for any /pd/ link; on later pages it
            # blocks until the first card's productId differs from prev_first_pid.
            rendered_ok = await self._wait_for_listing_render(prev_first_pid, timeout_ms=15000)

            # Phase 5.6: check for Akamai/CF block BEFORE we conclude SPA-fail.
            # The "no new products after 15s" symptom could be a block page that
            # never renders product cards. Try one rotation+retry round; only
            # stop if the page is still blocked after recovery.
            from block_detector import detect_block
            block_verdict = await detect_block(self.page)
            if block_verdict["kind"] != "ok":
                recovered = await self._listing_handle_block(block_verdict, page_num)
                if not recovered:
                    break
                # Re-navigate to the same page on the rotated IP.
                await self._goto(page_url)
                rendered_ok = await self._wait_for_listing_render(prev_first_pid, timeout_ms=15000)
                # Re-check; if still blocked, give up on listing.
                block_verdict = await detect_block(self.page)
                if block_verdict["kind"] != "ok":
                    print(
                        f"[Listing] page {page_num} STILL blocked after rotation "
                        f"({block_verdict['kind']}: {block_verdict['detail']}) — stopping."
                    )
                    break

            # Lazy-load nudge: some Lowes paginated XHRs only fire after a
            # viewport interaction. If SPA-wait failed, scroll the page in
            # steps and re-check whether #listItems populates with new pids
            # before declaring the page truly empty. Cheap (≤6s) and only
            # runs on the SPA-fail path, so unaffected pages aren't slowed.
            if not rendered_ok and prev_first_pid:
                print(f"  [Page {page_num}] SPA-wait fail — trying scroll-nudge to trigger XHR...")
                try:
                    await self.page.evaluate(
                        """
                        async () => {
                            const h = () => document.body.scrollHeight;
                            for (let i = 0; i < 6; i++) {
                                window.scrollTo(0, h() * (i + 1) / 5);
                                await new Promise(r => setTimeout(r, 600));
                            }
                            window.scrollTo(0, 0);
                        }
                        """
                    )
                except Exception:
                    pass
                rendered_ok = await self._wait_for_listing_render(prev_first_pid, timeout_ms=8000)
                if rendered_ok:
                    print(f"  [Page {page_num}] scroll-nudge succeeded — #listItems populated")

            # GR_* collection check (2026-05-18). Confirmed via HTML dump:
            # when a narrow `rangeFacet=` filter exhausts real products,
            # Lowes fills subsequent pages with collection/bundle cards
            # (data-id="GR_*", href="/collections/..."). #listItems is
            # NON-empty but contains zero `/pd/` links, so SPA-wait scoped
            # to `/pd/` rightly times out. Distinguish this from a true
            # block/cache stale: when #listItems has GR_* tiles but no
            # real products, stop cleanly with an explicit message rather
            # than the misleading "SPA did not render" warning.
            if not rendered_ok and prev_first_pid:
                try:
                    gr_count = await self.page.evaluate(
                        """() => {
                            const scope = document.querySelector('#listItems');
                            if (!scope) return 0;
                            return scope.querySelectorAll(
                                'div.js-save-to-list[data-id^="GR_"], a[href*="/collections/"]'
                            ).length;
                        }"""
                    )
                except Exception:
                    gr_count = 0
                if gr_count and gr_count >= 3:
                    print(
                        f"  [Page {page_num}] #listItems contains {gr_count} collection "
                        f"(GR_*) cards but ZERO real /pd/ products. This category's "
                        f"single-product results are exhausted at offset "
                        f"{start_offset + (page_num - page_from) * 24} — remaining pages "
                        f"are bundle/collection cards which we don't scrape. Stopping."
                    )
                    break

            if not rendered_ok and prev_first_pid:
                # Diagnostic: dump what's actually on the page so user can tell
                # whether (a) Lowes clamped offset back to page 1 (genuine end
                # of category), (b) Akamai served a stale page, or (c) the
                # SPA-wait raced. Without this evidence we can't distinguish
                # "real end-of-category" from "false-positive break".
                try:
                    diag = await self.page.evaluate(
                        r"""() => {
                            const pids_doc = Array.from(document.querySelectorAll('a[href*="/pd/"]'))
                                .map(a => (a.getAttribute('href').match(/\/pd\/[^/]+\/(\d+)/) || [])[1])
                                .filter(Boolean);
                            const seen = new Set(); const uniq = [];
                            for (const p of pids_doc) { if (!seen.has(p)) { seen.add(p); uniq.push(p); } }
                            const scope = document.querySelector('#listItems');
                            const pids_list = scope ? Array.from(scope.querySelectorAll('a[href*="/pd/"]'))
                                .map(a => (a.getAttribute('href').match(/\/pd\/[^/]+\/(\d+)/) || [])[1])
                                .filter(Boolean) : [];
                            const seen2 = new Set(); const uniq2 = [];
                            for (const p of pids_list) { if (!seen2.has(p)) { seen2.add(p); uniq2.push(p); } }
                            const countEl = document.querySelector('[data-selector="splp-prd-count"], .results-count, [class*="ResultsCount"]');
                            return {
                                url: location.href,
                                title: document.title,
                                doc_first5: uniq.slice(0, 5),
                                list_first5: uniq2.slice(0, 5),
                                list_count: uniq2.length,
                                results_text: countEl ? countEl.textContent.trim().slice(0, 120) : null,
                            };
                        }"""
                    )
                    print(
                        f"  [Diag] Page {page_num} SPA-wait failed. "
                        f"playwright_url={diag.get('url')} title={diag.get('title')!r}"
                    )
                    print(
                        f"  [Diag] #listItems pids (first 5 of {diag.get('list_count')}): "
                        f"{diag.get('list_first5')}"
                    )
                    print(f"  [Diag] doc /pd/ pids (first 5): {diag.get('doc_first5')}")
                    if diag.get("results_text"):
                        print(f"  [Diag] results header: {diag['results_text']!r}")
                except Exception as e:
                    print(f"  [Diag] failed to read page state: {e}")
                # Dump full HTML so user can inspect what Lowes actually
                # returned for this offset (the diagnostic above only
                # snapshots a few selectors). File path is printed so user
                # can attach it directly when reporting.
                try:
                    html = await self.page.content()
                    os.makedirs(DEBUG_DIR, exist_ok=True)
                    html_path = os.path.join(
                        DEBUG_DIR,
                        f"listing_spafail_{_safe_name(category_name)}_p{page_num}_{int(time.time())}.html",
                    )
                    with open(html_path, "w", encoding="utf-8") as f:
                        f.write(html)
                    print(f"  [Diag] saved page HTML → {html_path}")
                except Exception as e:
                    print(f"  [Diag] HTML dump failed: {e}")
                print(
                    f"  [Done] Page {page_num}: SPA did not render new products within 15s "
                    f"(first card still has productId={prev_first_pid}). "
                    f"Stopping — likely end of category or persistent block."
                )
                break

            # 1) Try __NEXT_DATA__ (primary)
            nd = await self._next_data()
            products = []
            if nd:
                products = self._parse_listing_next_data(nd)
                if page_num == page_from and not products:
                    await self._save_debug(f"listing_{_safe_name(category_name)}", nd)

            # 2) Fallback to DOM if __NEXT_DATA__ gave nothing
            if not products:
                print("  [Fallback] Extracting from DOM...")
                # Phase 5.5: Lowes lazy-loads product tiles as they enter view.
                # One scroll-to-bottom often isn't enough — scroll in steps so
                # every tile passes through the viewport, then return to top.
                # Without this, pages came back with 11-17 real products
                # instead of the full 24 (per user 2026-05-17).
                await self.page.evaluate(
                    """
                    async () => {
                        const h = () => document.body.scrollHeight;
                        let last = 0;
                        for (let i = 0; i < 6; i++) {
                            window.scrollTo(0, h() * (i + 1) / 5);
                            await new Promise(r => setTimeout(r, 700));
                            if (h() === last) break;
                            last = h();
                        }
                        window.scrollTo(0, 0);
                    }
                    """
                )
                await asyncio.sleep(1)
                products = await self._parse_listing_dom()

            # Empty page — could be (a) genuine end of category or (b) soft block.
            # We surface the warning instead of silently breaking on the first
            # empty page; if a second empty page follows we treat it as truly
            # done (no more retries, to avoid wasting time on a real block).
            if not products:
                consecutive_empty += 1
                if consecutive_empty == 1 and page_num > page_from:
                    print(
                        f"  [Warn] Page {page_num} returned 0 products. "
                        f"Could be end-of-category OR soft block. Retrying once..."
                    )
                    await _delay(4, 8)
                    page_num += 1
                    continue
                print(
                    f"  [Done] {consecutive_empty} consecutive empty page(s) at "
                    f"{page_num}. Stopping (likely end of category or persistent block)."
                )
                break
            consecutive_empty = 0

            extracted_count = len(products)

            # Remember the first card's productId for next iteration's SPA-wait.
            # Use raw `products` order so it matches what _first_product_id_on_page
            # reads from DOM (querySelector picks the first /pd/ link in source).
            if products:
                pid0 = str(products[0].get("productId") or "")
                if pid0:
                    prev_first_pid = pid0

            # Diagnostic: print productIds extracted so user can verify whether
            # Lowes is actually paginating (different pids each page) or serving
            # the same window (shuffled order) — which would explain dup=20 every
            # page despite the SPA-wait succeeding.
            pid_sample = [str(p.get("productId") or "")[:12] for p in products[:5]]
            print(f"  [Page {page_num}] first 5 pids: {pid_sample}")

            # Phase 5.5: surface GR_* / no-pd-url tiles that may have been
            # filtered at the extraction layer. If user sees this >0 they know
            # Lowes' grid included non-product tiles (bundle/collection cards).
            group_dropped = sum(
                1 for p in products
                if str(p.get("productId") or "").startswith("GR_")
                or "/pd/" not in (p.get("productUrl") or "")
            )

            # Phase 5.6: in-code price filter removed. Pass the price range in
            # the Lowes URL itself (rangeFacet=price_td__min__max). Every
            # extracted item now flows through to dedup + enqueue.

            # Dedup against what's already in JSON (resume safety)
            new_products = []
            dup = 0
            for p in products:
                pid = str(p.get("productId", ""))
                if not pid:
                    continue
                if pid in seen_ids:
                    dup += 1
                    continue
                seen_ids.add(pid)
                new_products.append(p)
            all_products.extend(new_products)

            print(
                f"  [Page {page_num}] extracted={extracted_count} "
                f"group-cards={group_dropped} "
                f"dup={dup} new={len(new_products)} "
                f"=> total={len(all_products)}"
            )

            # Save incrementally (after each page)
            _save_json(out_file, {
                "category_name": category_name,
                "category_url": category_url,
                "scraped_at": datetime.now().isoformat(),
                "total": len(all_products),
                "products": all_products,
            })

            # Enqueue into job queue. We enqueue the FULL page slice (not just
            # `new_products`) so that if a previous run crashed mid-enqueue, the
            # SQLite INSERT OR IGNORE will pick up any rows that made it into
            # JSON but not into the queue.
            if self.state is not None and products:
                jobs = []
                for p in products:
                    pid = str(p.get("productId") or "")
                    purl = p.get("productUrl") or ""
                    if pid and purl:
                        jobs.append((pid, purl))
                if jobs:
                    ins, skp = self.state.enqueue_listing(category_name, jobs)
                    print(f"  [Queue] +{ins} new jobs, {skp} already queued")

            # Phase 5.5 update: do NOT early-break on an all-dup page.
            # Subcategories typically overlap heavily with the parent (page 1
            # can be 100% dup) but page 3-5 often has unique items the parent
            # paginator never reached (Lowes caps at ~25 pages per listing).
            # Per user 2026-05-17: "đảm bảo scrape ở category khi thấy duplicate
            # thì bỏ qua nhưng bắt buộc phải scrape hết các page".
            # Natural stop conditions still apply: page_to limit, empty page
            # streak, SPA-wait timeout (prev_first_pid unchanged after 15s).
            if dup == extracted_count and extracted_count > 0:
                print(
                    f"  [Page {page_num}] all {dup} items already seen — "
                    f"continuing to next page (subcategory overlap)"
                )

            # No more pagination probe here — _click_next_listing() at the top
            # of the next iteration both probes and advances. If it returns
            # False, the loop breaks. Just delay and bump the counter.
            await _delay()
            page_num += 1

        # After listing completes, leave Referer pointing at the category URL.
        # If the same scraper instance moves on to scrape_details_for_category
        # next (single-worker `full` action), every /pd/ visit will then have
        # Referer=category — looks like normal user navigation.
        self._referer = base

        print(f"\n[Listing] Complete: {len(all_products)} products for '{category_name}'")
        return category_name, all_products

    def _parse_listing_next_data(self, nd):
        """Try multiple paths to find product list in __NEXT_DATA__."""
        products = []
        try:
            pp = nd.get("props", {}).get("pageProps", {})

            # Try known paths (Lowes uses different structures)
            raw_list = None
            for path in [
                lambda: pp.get("productList", {}).get("products"),
                lambda: pp.get("productList", {}).get("items"),
                lambda: pp.get("products"),
                lambda: pp.get("data", {}).get("productList", {}).get("products"),
                lambda: pp.get("initialData", {}).get("searchServiceResult", {}).get("products"),
                lambda: pp.get("componentsData", {}).get("productList", {}).get("products"),
            ]:
                try:
                    result = path()
                    if result and isinstance(result, list) and len(result) > 0:
                        raw_list = result
                        break
                except Exception:
                    continue

            if not raw_list:
                # Deep search: find any list of dicts with 'productId' key
                raw_list = self._deep_find_products(pp)

            if not raw_list:
                return []

            for item in raw_list:
                if not isinstance(item, dict):
                    continue
                pid = str(
                    item.get("productId") or item.get("id") or item.get("omniItemId") or ""
                )
                purl = item.get("url") or item.get("pdURL") or item.get("pdpURL") or ""
                # Phase 5.5: skip Lowes "group cards" (bundle/collection tiles
                # that share the product-tile DOM but have pid like "GR_4042"
                # and no /pd/ link). They pollute listing JSON and get silently
                # dropped at enqueue anyway because their url is empty.
                if pid.startswith("GR_") or "/pd/" not in purl:
                    continue
                products.append({
                    "productId": pid,
                    "name": item.get("name") or item.get("title") or item.get("description", ""),
                    "brand": item.get("brand") or item.get("brandName"),
                    "price": item.get("price") or self._dig(item, "pricing", "price"),
                    "wasPrice": item.get("wasPrice") or self._dig(item, "pricing", "wasPrice"),
                    "rating": item.get("rating") or item.get("starRating") or item.get("averageRating"),
                    "reviewCount": item.get("reviewCount") or item.get("numberOfReviews"),
                    "imageUrl": item.get("imageUrl") or item.get("image") or self._dig(item, "images", 0),
                    "productUrl": purl,
                    "modelNumber": item.get("modelNumber") or item.get("modelNo"),
                })

        except Exception as e:
            print(f"  [Error] parse listing: {e}")

        return products

    def _deep_find_products(self, obj, depth=0):
        """Recursively find a list of dicts containing 'productId' or 'omniItemId'."""
        if depth > 5:
            return None
        if isinstance(obj, list) and len(obj) > 0:
            if isinstance(obj[0], dict) and any(
                k in obj[0] for k in ("productId", "omniItemId", "modelNumber")
            ):
                return obj
        if isinstance(obj, dict):
            for v in obj.values():
                result = self._deep_find_products(v, depth + 1)
                if result:
                    return result
        return None

    def _dig(self, d, *keys):
        for k in keys:
            if isinstance(d, dict):
                d = d.get(k)
            elif isinstance(d, list) and isinstance(k, int) and k < len(d):
                d = d[k]
            else:
                return None
        return d

    async def _parse_listing_dom(self):
        """Fallback: extract products from DOM using Lowes-specific selectors.
        
        Primary source: div.js-save-to-list data-* attributes (most reliable).
        Scope: #listItems section (the actual product grid, excludes sponsored).
        """
        return await self.page.evaluate(r"""
            () => {
                // 1) Scope to #listItems — the definitive product grid
                const scope = document.querySelector('#listItems') || document;

                // 2) Primary strategy: extract from div.js-save-to-list data attributes
                //    Each product tile has one of these with data-id, data-brand, etc.
                const saveToListDivs = scope.querySelectorAll('div.js-save-to-list');
                const seenPids = new Set();
                const results = [];

                for (const stl of saveToListDivs) {
                    const pid = stl.getAttribute('data-id');
                    if (!pid || seenPids.has(pid)) continue;
                    // Phase 5.5: skip Lowes "group cards" (bundle/collection
                    // tiles, pid like "GR_4042"). They aren't real /pd/ pages.
                    if (pid.startsWith('GR_')) continue;
                    seenPids.add(pid);

                    const brand = stl.getAttribute('data-brand') || '';
                    const description = stl.getAttribute('data-description') || '';
                    const imagePath = stl.getAttribute('data-imagepath') || '';
                    const modelNumber = stl.getAttribute('data-modelid') || '';
                    const itemNumber = stl.getAttribute('data-itemnumber') || '';

                    // Find the tile container for this product to get price/rating/url
                    const tileContainer = stl.closest('[data-tile]') 
                        || stl.closest('[data-selector="prd-image-holder"]')?.parentElement;
                    const tileNum = tileContainer?.getAttribute('data-tile');

                    // Product URL: find the /pd/ link near this product
                    let productUrl = '';
                    const pdLink = scope.querySelector(
                        `[id="${pid}"] a[href*="/pd/"]`
                    );
                    if (pdLink) {
                        productUrl = pdLink.getAttribute('href') || '';
                    }

                    // Price: use data-selector="splp-prd-act-$" within the same tile group
                    let price = '';
                    let wasPrice = '';
                    if (tileNum) {
                        const priceHolder = scope.querySelector(
                            `[data-tile="${tileNum}"][data-selector="prd-price-holder"]`
                        );
                        if (priceHolder) {
                            const actEl = priceHolder.querySelector('[data-selector="splp-prd-act-$"]');
                            if (actEl) {
                                const al = actEl.getAttribute('aria-label') || '';
                                const m = al.match(/\$[\d,.]+/);
                                if (m) price = m[0];
                            }
                            if (!price) {
                                const rngEl = priceHolder.querySelector('[data-selector="splp-prd-rng"]');
                                if (rngEl) {
                                    const al = rngEl.getAttribute('aria-label') || '';
                                    if (al) price = al;
                                }
                            }
                            const wasEl = priceHolder.querySelector('[data-selector="splp-prd-promo-was-$"]');
                            if (wasEl) {
                                const al = wasEl.getAttribute('aria-label') || '';
                                const m = al.match(/\$[\d,.]+/);
                                if (m) wasPrice = m[0];
                            }
                        }
                    }

                    // Rating & review count
                    let rating = '';
                    let reviewCount = '';
                    if (tileNum) {
                        const ratingHolder = scope.querySelector(
                            `[data-tile="${tileNum}"][data-selector="prd-ratings-holder"]`
                        );
                        if (ratingHolder) {
                            const rn = ratingHolder.querySelector('.rating-number');
                            if (rn) rating = rn.textContent.trim();
                            const rc = ratingHolder.querySelector('.rating-count');
                            if (rc) reviewCount = rc.textContent.trim();
                        }
                    }

                    // Phase 5.5: skip if no /pd/ link was found — without a
                    // product URL this row can never be enqueued for detail
                    // scrape and just bloats listing JSON.
                    if (!productUrl || productUrl.indexOf('/pd/') < 0) {
                        seenPids.delete(pid);
                        continue;
                    }

                    results.push({
                        productId: pid,
                        name: description,
                        brand: brand,
                        price: price,
                        wasPrice: wasPrice,
                        rating: rating,
                        reviewCount: reviewCount,
                        productUrl: productUrl,
                        imageUrl: imagePath,
                        modelNumber: modelNumber,
                        itemNumber: itemNumber,
                    });
                }

                // 3) Supplement (Phase 5.5): ALWAYS sweep /pd/ links to catch
                //    real products that don't have `div.js-save-to-list`
                //    (Lowes' grid layout sometimes nests product cards under
                //    different containers). Previous threshold `< 10` meant a
                //    page with 11-17 js-save-to-list tiles never triggered the
                //    sweep, so we missed 7-13 real products per page.
                //    Scope is broadened to whole document because some tiles
                //    sit outside #listItems (e.g. featured rows). seenPids
                //    dedup prevents double-counting.
                {
                    const pdLinks = document.querySelectorAll('a[href*="/pd/"]');
                    for (const a of pdLinks) {
                        const href = a.getAttribute('href') || '';
                        const pidMatch = href.match(/\/pd\/[^/]+\/(\d+)/);
                        if (!pidMatch) continue;
                        const pid = pidMatch[1];
                        if (seenPids.has(pid)) continue;
                        seenPids.add(pid);
                        const slug = href.split('/pd/')[1]?.split('/')[0] || '';
                        results.push({
                            productId: pid,
                            name: slug.replace(/-/g, ' '),
                            brand: '',
                            price: '',
                            wasPrice: '',
                            productUrl: href,
                            imageUrl: '',
                        });
                    }
                }

                return results;
            }
        """)

    # ── Scrape Product Details ───────────────────────────────────────────

    async def _expand_accordions(self):
        """Click accordion headers to expand lazy-loaded content (Specs, Overview, Features).
        Lowes uses data-testid attributes and lazy-loads spec tables on click."""
        sections = [
            ("specification-accordion", "Specifications"),
            ("overview-accordion", "Overview"),
            ("productfeature-accordion", "Product Features"),
        ]
        for testid, label in sections:
            try:
                accordion = self.page.locator(f'[data-testid="{testid}"]')
                if await accordion.count() == 0:
                    continue
                # Check if already opened (has class 'opened')
                cls = await accordion.get_attribute("class") or ""
                if "opened" not in cls:
                    btn = accordion.locator("button.accordion-header")
                    if await btn.count() > 0:
                        await btn.first.scroll_into_view_if_needed()
                        await btn.first.click()
                        # Wait for lazy-loaded content
                        if "spec" in testid:
                            try:
                                await accordion.locator("table").first.wait_for(state="visible", timeout=8000)
                            except Exception:
                                pass
                        elif "productfeature" in testid:
                            # Features load via Salsify iframe
                            try:
                                await accordion.locator("iframe").first.wait_for(state="attached", timeout=8000)
                                await asyncio.sleep(3)  # iframe content needs time to render
                            except Exception:
                                pass
                        await asyncio.sleep(1)
            except Exception as e:
                # If the underlying browser is gone, stop logging per-section
                # and propagate so worker can reconnect. Otherwise log and
                # continue with the next accordion.
                if _is_browser_closed_error(e):
                    raise BrowserClosedError(
                        f"browser closed during accordion expand ({label}): {e}"
                    ) from e
                if _is_page_crashed_error(e):
                    # Same reasoning as browser-closed: don't keep clicking
                    # accordions on a dead tab. Worker recreates the page.
                    raise PageCrashedError(
                        f"page crashed during accordion expand ({label}): {e}"
                    ) from e
                print(f"  [Warn] Accordion {label}: {e}")
        # Also try to expand Product Features
        try:
            feature_btns = self.page.locator('button.accordion-header')
            count = await feature_btns.count()
            for i in range(count):
                btn = feature_btns.nth(i)
                text = await btn.text_content()
                if text and "product feature" in text.lower():
                    parent = btn.locator('..')
                    parent_cls = await parent.get_attribute("class") or ""
                    if "opened" not in parent_cls:
                        await btn.scroll_into_view_if_needed()
                        await btn.click()
                        await asyncio.sleep(1)
                    break
        except Exception as e:
            if _is_browser_closed_error(e):
                raise BrowserClosedError(
                    f"browser closed during feature-button scan: {e}"
                ) from e
            if _is_page_crashed_error(e):
                raise PageCrashedError(
                    f"page crashed during feature-button scan: {e}"
                ) from e

    async def scrape_detail(self, product_url, product_id, force_refresh=False):
        """Scrape a single product detail page.

        If an existing JSON is found, the new scrape result is merged into it via
        completeness.merge_detail (static fields preserved, dynamic fields refreshed,
        dicts/lists merged). Callers can inspect completeness via
        completeness.check_completeness(returned_dict).

        force_refresh=False keeps the legacy short-circuit: if the existing file is
        already complete, return it without touching the network.
        """
        from completeness import check_completeness, merge_detail

        pid = str(product_id)
        # Phase 5.5: look up the existing file under either name variant
        # ({pid}.json or {pid}_missimage.json). Save path is chosen later
        # based on whether the new scrape actually has images.
        existing_path = detail_path_existing(pid)
        existing = _load_json(existing_path) if existing_path else None

        # Fast path: existing + complete + caller didn't ask for refresh
        if existing and not force_refresh:
            is_complete, _missing = check_completeness(existing)
            if is_complete:
                return existing

        await self._goto(product_url)

        # Expand accordion sections (Overview, Specs, Features)
        await self._expand_accordions()

        detail = {
            "productId": pid,
            "url": product_url if product_url.startswith("http") else f"{BASE_URL}{product_url}",
            "scraped_at": datetime.now().isoformat(),
        }

        # 1) Extract structured data from JSON-LD (most reliable for GTIN/UPC)
        jsonld = await self.page.evaluate("""
            () => {
                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d['@type'] === 'Product' || d.gtin || d.gtin12 || d.gtin13 || d.sku) return d;
                    } catch(e) {}
                }
                return null;
            }
        """)
        if jsonld:
            detail["gtin"] = jsonld.get("gtin") or jsonld.get("gtin12") or jsonld.get("gtin13") or ""
            detail["sku"] = jsonld.get("sku") or ""
            detail["mpn"] = jsonld.get("mpn") or ""
            if isinstance(jsonld.get("brand"), dict):
                detail["brand"] = jsonld["brand"].get("name", "")
            elif isinstance(jsonld.get("brand"), str):
                detail["brand"] = jsonld["brand"]

        # 2) __NEXT_DATA__ for additional structured data
        nd = await self._next_data()
        if nd:
            pp = nd.get("props", {}).get("pageProps", {})
            # Try to find GTIN/UPC in __NEXT_DATA__ if not in JSON-LD
            if not detail.get("gtin"):
                gtin = self._deep_find_key(pp, ["gtin", "gtin12", "gtin13", "upc", "UPC", "GTIN"])
                if gtin:
                    detail["gtin"] = str(gtin)

        # 3) DOM extraction with corrected selectors
        dom = await self.page.evaluate("""
            () => {
                const r = {};

                // Title
                const h1 = document.querySelector('h1');
                r.title = h1 ? h1.textContent.trim() : '';

                // Brand - multiple strategies
                r.brand = '';
                // Strategy 1: "Shop BRAND" link
                const brandLink = document.querySelector('a[href*="/brand/"]');
                if (brandLink) {
                    r.brand = brandLink.textContent.trim().replace(/^shop\\s+/i, '');
                }
                // Strategy 2: link containing "Shop" text near header
                if (!r.brand) {
                    const shopLinks = document.querySelectorAll('a');
                    for (const a of shopLinks) {
                        const t = a.textContent.trim();
                        if (t.startsWith('Shop ') && t.length < 40) {
                            r.brand = t.replace(/^Shop\\s+/i, '');
                            break;
                        }
                    }
                }
                // Strategy 3: extract from title (first word or known brand)
                if (!r.brand && r.title) {
                    const knownBrands = ['DEWALT','CRAFTSMAN','Kobalt','Metabo','Milwaukee','Bosch','Makita','SKIL','BLACK+DECKER','Husqvarna','EGO','Greenworks','FLEX','HART','Dremel','Porter-Cable','Ridgid','Ryobi','Hitachi'];
                    for (const b of knownBrands) {
                        if (r.title.toUpperCase().startsWith(b.toUpperCase())) {
                            r.brand = b;
                            break;
                        }
                    }
                    // Fallback: first word of title
                    if (!r.brand) r.brand = r.title.split(' ')[0];
                }

                // Price - find leaf text nodes with $ pattern
                r.price = '';
                r.wasPrice = '';
                const priceWalker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
                const foundPrices = [];
                while (priceWalker.nextNode()) {
                    const t = priceWalker.currentNode.textContent.trim();
                    if (/^\\$[\\d,]+/.test(t) && t.length < 20) {
                        foundPrices.push(t);
                    }
                }
                if (foundPrices.length >= 1) r.price = foundPrices[0];
                if (foundPrices.length >= 2) r.wasPrice = foundPrices[1];

                // Model & Item number
                r.modelNumber = '';
                r.itemNumber = '';
                const modelEl = document.querySelector('[class*="model" i], [class*="item-number" i]');
                if (modelEl) {
                    const text = modelEl.textContent.trim();
                    const modelMatch = text.match(/Model\\s*#?\\s*([\\w-]+)/i);
                    const itemMatch = text.match(/Item\\s*#?\\s*([\\w-]+)/i);
                    if (modelMatch) r.modelNumber = modelMatch[1];
                    if (itemMatch) r.itemNumber = itemMatch[1];
                }

                // Rating & Review Count - from header area
                r.rating = '';
                r.reviewCount = '';
                // Look for star rating element or aria-label
                const ratingEl = document.querySelector('[aria-label*="stars" i], [aria-label*="rating" i], [class*="stars" i]');
                if (ratingEl) {
                    const ariaLabel = ratingEl.getAttribute('aria-label') || '';
                    const rMatch = ariaLabel.match(/(\\d+\\.?\\d*)\\s*(out of|stars|star)/i);
                    if (rMatch) r.rating = rMatch[1];
                }
                if (!r.rating) {
                    // Try text near stars
                    const allEls = document.querySelectorAll('[class*="rating" i], [class*="stars" i]');
                    for (const el of allEls) {
                        const t = el.textContent.trim();
                        const m = t.match(/^(\\d+\\.\\d+)/);
                        if (m) { r.rating = m[1]; break; }
                    }
                }
                // Review count - number in parentheses near rating
                const reviewLinks = document.querySelectorAll('a[href*="review" i], button[class*="review" i], [class*="review" i] a');
                for (const el of reviewLinks) {
                    const t = el.textContent.trim();
                    const m = t.match(/(\\d+)/);
                    if (m && parseInt(m[1]) > 0) { r.reviewCount = m[1]; break; }
                }

                // Overview / Description - from accordion content
                r.description = '';
                r.descriptionBullets = [];
                // Find expanded Overview section content
                const allSections = document.querySelectorAll('[class*="accordion" i] [class*="content" i], [class*="accordion" i] [class*="body" i], [class*="accordion" i] [role="region"], [aria-labelledby]');
                for (const section of allSections) {
                    const prevBtn = section.previousElementSibling || section.closest('[class*="accordion"]')?.querySelector('button');
                    const label = prevBtn?.textContent?.toLowerCase() || section.getAttribute('aria-label')?.toLowerCase() || '';
                    if (label.includes('overview')) {
                        // Get paragraph text
                        const paras = section.querySelectorAll('p');
                        const paraTexts = [...paras].map(p => p.textContent.trim()).filter(t => t.length > 20);
                        r.description = paraTexts.join(' ');

                        // Get bullet points
                        const lis = section.querySelectorAll('li');
                        r.descriptionBullets = [...lis].map(li => li.textContent.trim()).filter(t => t);
                        break;
                    }
                }
                // Fallback: try aria-controls
                if (!r.description) {
                    const overviewBtns = document.querySelectorAll('button, [role="button"]');
                    for (const btn of overviewBtns) {
                        if ((btn.textContent || '').toLowerCase().includes('overview')) {
                            const controlsId = btn.getAttribute('aria-controls');
                            if (controlsId) {
                                const panel = document.getElementById(controlsId);
                                if (panel) {
                                    const paras = panel.querySelectorAll('p');
                                    r.description = [...paras].map(p => p.textContent.trim()).filter(t => t.length > 20).join(' ');
                                    r.descriptionBullets = [...panel.querySelectorAll('li')].map(li => li.textContent.trim()).filter(t => t);
                                }
                            }
                            break;
                        }
                    }
                }

                // Product Features - placeholder (actual extraction via iframe below)
                r.featureText = [];
                r.featureImages = [];
                // Try direct DOM first (some products have inline features)
                const featureAccordion = document.querySelector('[data-testid="productfeature-accordion"]');
                if (featureAccordion) {
                    const ac = featureAccordion.querySelector('span.accordion-content, div.accordion-content');
                    if (ac) {
                        // Direct text/images (non-iframe products)
                        const directImgs = ac.querySelectorAll('img:not([src*="data:"])');
                        r.featureImages = [...directImgs]
                            .map(i => i.src || i.getAttribute('data-src') || '')
                            .filter(u => u && u.startsWith('http'));
                        const texts = ac.querySelectorAll('p, li, h3, h4');
                        r.featureText = [...texts]
                            .map(t => t.textContent.trim())
                            .filter(t => t.length > 10);
                    }
                }

                // Specifications - Lowes stores specs in SpecificationComponent wrapper
                // Content exists in DOM even when accordion is collapsed (aria-hidden)
                // Structure: tables with 4-col rows, keys in h6, values in p
                r.specifications = {};
                // Strategy 1: Find the SpecificationComponent wrapper directly
                let specWrapper = document.querySelector('[class*="SpecificationComponent"], [class*="NewSpecificationWrapper"]');
                // Strategy 2: Find accordion-content span after "Specifications" button
                if (!specWrapper) {
                    for (const ac of document.querySelectorAll('span.accordion-content, div.accordion-content')) {
                        const prev = ac.previousElementSibling;
                        if ((prev?.textContent || '').toLowerCase().includes('specification')) {
                            specWrapper = ac;
                            break;
                        }
                    }
                }
                if (specWrapper) {
                    const specRows = specWrapper.querySelectorAll('table tbody tr');
                    for (const row of specRows) {
                        const cells = row.querySelectorAll('td');
                        if (cells.length === 0) continue;
                        // 4-column layout: td(h6=key), td(p=val), td(h6=key), td(p=val)
                        if (cells.length >= 4) {
                            const k1 = (cells[0].querySelector('h6') || cells[0]).textContent.trim();
                            const v1 = (cells[1].querySelector('p') || cells[1]).textContent.trim();
                            const k2 = (cells[2].querySelector('h6') || cells[2]).textContent.trim();
                            const v2 = (cells[3].querySelector('p') || cells[3]).textContent.trim();
                            if (k1 && v1 && k1.length < 80) r.specifications[k1] = v1;
                            if (k2 && v2 && k2.length < 80) r.specifications[k2] = v2;
                        } else if (cells.length >= 2) {
                            const k = (cells[0].querySelector('h6') || cells[0]).textContent.trim();
                            const v = (cells[1].querySelector('p') || cells[1]).textContent.trim();
                            if (k && v && k.length < 80) r.specifications[k] = v;
                        }
                    }
                }

                // Product images - from #mfe-gallery only
                // Lowes gallery uses img.tile-img with ?size=pdhism for main images
                const imgSet = new Set();
                const productImgs = [];
                const gallery = document.querySelector('#mfe-gallery');
                if (gallery) {
                    gallery.querySelectorAll('img.tile-img').forEach(img => {
                        let src = img.src || img.getAttribute('data-src') || '';
                        if (!src || src.includes('data:')) return;
                        // Get full-size URL (remove size param)
                        src = src.replace(/\?size=\w+/, '');
                        // Normalize protocol
                        if (src.startsWith('//')) src = 'https:' + src;
                        if (!imgSet.has(src)) {
                            imgSet.add(src);
                            productImgs.push(src);
                        }
                    });
                }
                // Fallback: thumbnail images if no tile-img found
                if (productImgs.length === 0 && gallery) {
                    gallery.querySelectorAll('img[src*="mobileimages.lowes.com"]').forEach(img => {
                        let src = img.src || '';
                        src = src.replace(/\?size=\w+/, '');
                        if (src.startsWith('//')) src = 'https:' + src;
                        if (src && !imgSet.has(src) && !src.includes('thumbnail')) {
                            imgSet.add(src);
                            productImgs.push(src);
                        }
                    });
                }
                r.images = productImgs;

                return r;
            }
        """)

        # Merge DOM data
        detail["title"] = dom.get("title", "")
        detail["brand"] = detail.get("brand") or dom.get("brand", "")
        detail["price"] = dom.get("price", "")
        detail["wasPrice"] = dom.get("wasPrice", "")
        detail["modelNumber"] = dom.get("modelNumber", "")
        detail["itemNumber"] = dom.get("itemNumber", "")
        detail["rating"] = dom.get("rating", "")
        detail["reviewCount"] = dom.get("reviewCount", "")
        detail["description"] = dom.get("description", "")
        detail["descriptionBullets"] = dom.get("descriptionBullets", [])
        detail["featureText"] = dom.get("featureText", [])
        detail["featureImages"] = dom.get("featureImages", [])
        detail["specifications"] = dom.get("specifications", {})
        detail["images"] = dom.get("images", [])

        # 4) Extract features from Salsify iframe (if present)
        if not detail["featureImages"] and not detail["featureText"]:
            try:
                iframe_loc = self.page.frame_locator('#salsify-ec-iframe')
                # Check if iframe exists
                iframe_check = self.page.locator('#salsify-ec-iframe')
                if await iframe_check.count() > 0:
                    # Extract images from inside the iframe
                    imgs = iframe_loc.locator('img')
                    img_count = await imgs.count()
                    feat_imgs = []
                    feat_texts = []
                    for i in range(min(img_count, 20)):
                        src = await imgs.nth(i).get_attribute('src') or ''
                        if src and src.startswith('http') and 'data:' not in src:
                            feat_imgs.append(src)
                    # Extract text from inside the iframe
                    texts = iframe_loc.locator('p, h2, h3, h4, li, span')
                    text_count = await texts.count()
                    for i in range(min(text_count, 30)):
                        try:
                            t = await texts.nth(i).text_content()
                            if t and len(t.strip()) > 15:
                                feat_texts.append(t.strip())
                        except Exception:
                            pass
                    if feat_imgs:
                        detail["featureImages"] = feat_imgs
                    if feat_texts:
                        detail["featureText"] = feat_texts
            except Exception as e:
                pass  # iframe not available or cross-origin

        # Phase 5.5: if the freshly-scraped fields look like an Access-Denied
        # overlay (e.g. title="Access Denied"), DON'T merge-save — that would
        # corrupt good existing data and create a poisoned file that fools
        # check_completeness. Flag the result so the worker triggers recovery.
        from block_detector import result_looks_like_block
        block_in_result = result_looks_like_block(detail)
        if block_in_result is not None:
            print(
                f"  [{self.worker_id}] [scrape] result looks like block "
                f"({block_in_result['detail']}) — skipping save"
            )
            detail["_blocked"] = block_in_result
            return detail

        # Merge with existing file (Phase 2: gap-fill). If no existing, this is a no-op.
        if existing:
            detail = merge_detail(existing, detail)

        # Phase 5.5: pick filename based on whether the final merged record
        # actually has images. Missing images is NOT an error — the data is
        # saved under <pid>_missimage.json so the user can spot which products
        # need manual image upload. Clean up the OTHER variant if present so
        # we don't leave stale copies behind.
        std_path, miss_path = detail_paths(pid)
        has_images = bool(detail.get("images"))
        if has_images:
            save_path, stale_path = std_path, miss_path
        else:
            save_path, stale_path = miss_path, std_path
        _save_json(save_path, detail)
        if os.path.exists(stale_path):
            try:
                os.remove(stale_path)
            except OSError:
                pass
        if not has_images:
            print(
                f"  [{self.worker_id}] [scrape] {pid}: no images — saved as "
                f"{os.path.basename(save_path)} (manual image upload needed)"
            )
        return detail

    def reconcile_existing_files(self, category_name):
        """Walk listing JSON for a category and:
          - enqueue any productId not yet in DB
          - mark each job done/needs_refill based on its existing detail JSON

        Useful when migrating from pre-Phase-1 data (DB empty but files exist).
        Returns a summary dict.
        """
        from completeness import check_completeness

        if self.state is None:
            return {"error": "no state manager"}

        listing_file = os.path.join(LISTINGS_DIR, f"{_safe_name(category_name)}.json")
        listing = _load_json(listing_file)
        if not listing:
            return {"error": f"no listing file: {listing_file}"}

        products = listing.get("products", [])
        jobs = []
        for p in products:
            pid = str(p.get("productId") or "")
            purl = p.get("productUrl") or ""
            if pid and purl:
                jobs.append((pid, purl))
        ins, skp = self.state.enqueue_listing(category_name, jobs)

        done = refill = missing_file = 0
        for pid, _ in jobs:
            # Phase 5.5: file may live under {pid}.json or {pid}_missimage.json
            data = load_detail(pid)
            if not data:
                missing_file += 1
                continue
            is_complete, miss = check_completeness(data)
            if is_complete:
                self.state.mark_done(pid)
                done += 1
            else:
                self.state.mark_needs_refill(pid, miss)
                refill += 1

        summary = {
            "category": category_name,
            "enqueued_new": ins,
            "already_in_db": skp,
            "marked_done": done,
            "marked_needs_refill": refill,
            "no_detail_file": missing_file,
        }
        print(f"[Reconcile] {summary}")
        return summary

    def _deep_find_key(self, obj, keys, depth=0):
        """Recursively find the first value matching any of the given keys."""
        if depth > 6:
            return None
        if isinstance(obj, dict):
            for k in keys:
                if k in obj and obj[k]:
                    return obj[k]
            for v in obj.values():
                result = self._deep_find_key(v, keys, depth + 1)
                if result:
                    return result
        if isinstance(obj, list):
            for item in obj[:10]:
                result = self._deep_find_key(item, keys, depth + 1)
                if result:
                    return result
        return None

    async def scrape_details_for_category(self, category_url=None, category_name=None):
        """Scrape product details for a category, or for ALL pending jobs.

        Two modes:
          • With state manager (Phase 1+): pull jobs from SQLite queue, atomic claim,
            graceful Ctrl+C release. Cross-worker safe.
          • Without state manager (legacy): iterate listing JSON directly.

        Args:
            category_url:  full category URL (derives name + enables warmup).
            category_name: category name only (when the UI picked from the
                queue dropdown — no URL available). Skips warmup since we
                don't know which category page to use as Referer source.
                Both None → consume queue across ALL categories.

        Legacy (no-state) mode still requires a URL because it reads the
        on-disk listing JSON.
        """
        if category_url and not category_name:
            category_name = _name_from_url(category_url)

        # Phase 5a: warm up the session BEFORE hitting any /pd/ link, unless
        # we're already warmed (e.g. caller ran scrape_listing first which
        # built equivalent state). Cheap idempotency guard via _warmed_up.
        # Warmup requires a category URL — name-only mode skips it.
        if not self._warmed_up and category_url:
            await self.warmup(category_url)

        if self.state is not None:
            await self._scrape_details_via_queue(category_name)
        else:
            if not category_url:
                raise ValueError(
                    "legacy (no-state) mode requires category_url — "
                    "URL-less / name-only detail only works with the SQLite queue"
                )
            await self._scrape_details_legacy(category_name, category_url)

    async def _scrape_details_via_queue(self, category_name):
        """Phase 1+2: pull jobs from StateManager. Completeness-aware.

        `category_name=None` (2026-05-20): consume across all categories
        (claim_next ignores the category filter when None).
        """
        from completeness import check_completeness

        scope_label = f"'{category_name}'" if category_name else "ALL categories"
        print(f"\n[Detail] Worker {self.worker_id} consuming queue for {scope_label}...")
        scraped = 0
        skipped = 0
        refilled = 0
        partial = 0
        errors = 0
        current_pid = None  # for Ctrl+C release
        try:
            while True:
                if self._ui_stop is not None and self._ui_stop.is_set():
                    print(f"  [{self.worker_id}] Stop requested — exiting queue loop")
                    break
                job = self.state.claim_next(self.worker_id, category=category_name)
                if not job:
                    print(f"  [Queue] No more jobs for {scope_label}")
                    break
                current_pid = job["product_id"]
                pid = job["product_id"]
                purl = job["url"]
                was_refill = bool(job.get("missing_fields"))

                # Pre-check: if file already complete, mark done without scraping.
                # Phase 5.5: file may be under {pid}.json or {pid}_missimage.json.
                existing = load_detail(pid)
                if existing:
                    is_complete, _ = check_completeness(existing)
                    if is_complete:
                        self.state.mark_done(pid)
                        skipped += 1
                        print(f"  [{self.worker_id}] Complete: {pid}")
                        current_pid = None
                        continue

                try:
                    label = "Refill" if (existing or was_refill) else "Scraping"
                    print(f"  [{self.worker_id}] {label} {pid} ...")
                    result = await self.scrape_detail(purl, pid)
                    is_complete, missing = check_completeness(result)
                    if is_complete:
                        self.state.mark_done(pid)
                        if existing:
                            refilled += 1
                        else:
                            scraped += 1
                    else:
                        from config import MAX_REFILL_ATTEMPTS
                        this_attempt = (job.get("attempts") or 0) + 1
                        if this_attempt >= MAX_REFILL_ATTEMPTS:
                            err = (
                                f"partial after {this_attempt} attempts — "
                                f"missing {missing}"
                            )
                            self.state.mark_failed(pid, err)
                            errors += 1
                            print(f"  [{self.worker_id}] FAILED {pid}: {err}")
                        else:
                            self.state.mark_needs_refill(pid, missing)
                            partial += 1
                            print(
                                f"  [{self.worker_id}] Partial {pid} "
                                f"(attempt {this_attempt}/{MAX_REFILL_ATTEMPTS}): missing {missing}"
                            )
                    current_pid = None
                    await _delay()
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    print(f"  [{self.worker_id}] Error {pid}: {e}")
                    self.state.mark_failed(pid, str(e))
                    errors += 1
                    current_pid = None
        except KeyboardInterrupt:
            if current_pid:
                print(f"  [{self.worker_id}] Ctrl+C — releasing job {current_pid}")
                self.state.release(current_pid)
            raise
        finally:
            print(
                f"\n[Detail] {self.worker_id} done: "
                f"{scraped} scraped, {refilled} refilled, {partial} partial, "
                f"{skipped} skipped, {errors} errors"
            )

    async def _scrape_details_legacy(self, category_name, category_url):
        """Pre-Phase-1 behaviour, kept for the `detail` CLI command without a queue."""
        listing_file = os.path.join(LISTINGS_DIR, f"{_safe_name(category_name)}.json")
        listing = _load_json(listing_file)
        if not listing:
            print(f"[Error] No listing file: {listing_file}")
            print(f"  Run 'python scraper.py list {category_url}' first.")
            return

        products = listing.get("products", [])
        total = len(products)
        print(f"\n[Detail] Scraping {total} products for '{category_name}'...")

        scraped = 0
        skipped = 0
        errors = 0

        for i, p in enumerate(products, 1):
            pid = str(p.get("productId", ""))
            purl = p.get("productUrl", "")

            if not purl or not pid:
                print(f"  [{i}/{total}] Skip (no URL/ID)")
                skipped += 1
                continue

            # Phase 5.5: skip if either filename variant already exists
            if detail_path_existing(pid):
                print(f"  [{i}/{total}] Exists: {pid}")
                skipped += 1
                continue

            try:
                name = p.get("name", pid)[:55]
                print(f"  [{i}/{total}] {name}...")
                await self.scrape_detail(purl, pid)
                scraped += 1
                await _delay()
            except Exception as e:
                print(f"  [{i}/{total}] Error: {e}")
                errors += 1

        print(f"\n[Detail] Done: {scraped} scraped, {skipped} skipped, {errors} errors")


# ─── CLI ─────────────────────────────────────────────────────────────────

HELP = """
Lowes Scraper - Commands:
  python scraper.py discover                                        - Discover all categories
  python scraper.py setzip                                          - Set zip code to 10001
  python scraper.py list <url>                                      - Scrape all pages
  python scraper.py list <url> --pages 1-2                          - Scrape pages 1 to 2
  python scraper.py list <url> --pages 1                            - Scrape page 1 only
  python scraper.py detail <url>                                    - Scrape product details (queue-aware)
  python scraper.py full <url>                                      - Full pipeline
  python scraper.py full <url> --pages 1-3                          - Full pipeline, pages 1-3
  python scraper.py status                                          - Show job queue stats
  python scraper.py status <category-url>                           - Stats for one category

Examples:
  python scraper.py list https://www.lowes.com/pl/power-tools/4294607842
  python scraper.py list https://www.lowes.com/pl/power-tools/4294607842 --pages 1-2
  python scraper.py detail https://www.lowes.com/pl/power-tools/4294607842
  python scraper.py full https://www.lowes.com/pl/Refrigerators/4294857975 --pages 1-5
"""


def _print_stats(state, category=None):
    if category:
        s = state.stats(category)
        print(f"\n[Queue] Category: {category}")
        print(f"  pending      : {s['pending']}")
        print(f"  claimed      : {s['claimed']}")
        print(f"  done         : {s['done']}")
        print(f"  failed       : {s['failed']}")
        print(f"  needs_refill : {s['needs_refill']}")
        print(f"  TOTAL        : {s['total']}")
    else:
        cats = state.categories()
        if not cats:
            print("\n[Queue] Empty.")
            return
        print(f"\n[Queue] {len(cats)} categories:")
        hdr = f"  {'category':<35s} {'total':>6} {'done':>6} {'pend':>6} {'clm':>5} {'fail':>5} {'refill':>6}"
        print(hdr)
        print("  " + "─" * (len(hdr) - 2))
        for c in cats:
            print(f"  {c['category']:<35s} {c['total']:>6} {c['done'] or 0:>6} "
                  f"{c['pending'] or 0:>6} {c['claimed'] or 0:>5} "
                  f"{c['failed'] or 0:>5} {c['needs_refill'] or 0:>6}")


async def main():
    if len(sys.argv) < 2:
        print(HELP)
        return

    cmd = sys.argv[1]

    # `status` / `reconcile` don't need a browser
    if cmd == "status":
        state = StateManager()
        try:
            released = state.release_stale()
            if released:
                print(f"[State] Released {released} stale claims")
            if len(sys.argv) >= 3:
                _print_stats(state, _name_from_url(sys.argv[2]))
            else:
                _print_stats(state)
        finally:
            state.close()
        return

    if cmd == "reconcile":
        if len(sys.argv) < 3:
            print("Usage: python scraper.py reconcile <category-url>")
            return
        state = StateManager()
        try:
            scraper = LowesScraper(worker_id="cli", state=state)
            scraper.reconcile_existing_files(_name_from_url(sys.argv[2]))
        finally:
            state.close()
        return

    if not ADSPOWER_PROFILE_ID:
        print("[Error] Set ADSPOWER_PROFILE_ID in config.py first!")
        return

    state = StateManager()
    # Release any stale claims (e.g., from a previous crashed run)
    released = state.release_stale()
    if released:
        print(f"[State] Released {released} stale claims from previous run")

    scraper = LowesScraper(profile_id=ADSPOWER_PROFILE_ID, worker_id="w0", state=state)

    try:
        await scraper.connect()

        if cmd == "discover":
            await scraper.discover()

        elif cmd == "setzip":
            await scraper.set_zip()

        elif cmd == "list":
            if len(sys.argv) < 3:
                print("Usage: python scraper.py list <url> [--pages N or N-M]")
                return
            url = sys.argv[2]
            page_from, page_to = _parse_pages(sys.argv[3:])
            await scraper.scrape_listing(url, page_from, page_to)

        elif cmd == "detail":
            if len(sys.argv) < 3:
                print("Usage: python scraper.py detail <url>")
                return
            await scraper.scrape_details_for_category(sys.argv[2])

        elif cmd == "full":
            if len(sys.argv) < 3:
                print("Usage: python scraper.py full <url> [--pages N or N-M]")
                return
            url = sys.argv[2]
            page_from, page_to = _parse_pages(sys.argv[3:])
            await scraper.set_zip()
            await _delay(CATEGORY_DELAY, CATEGORY_DELAY + 5)
            cat_name, _ = await scraper.scrape_listing(url, page_from, page_to)
            await _delay(CATEGORY_DELAY, CATEGORY_DELAY + 5)
            await scraper.scrape_details_for_category(url)

        else:
            print(f"Unknown command: {cmd}")
            print(HELP)
    finally:
        await scraper.close()
        state.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Main] Interrupted by user. State saved.")
