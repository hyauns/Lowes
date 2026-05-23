"""
Phase 5.2/5.3 — Block and proxy-failure recovery ladders.

Two distinct triggers, two ladders:

  • Akamai/CF block detected on page  →  _handle_block (Worker)
       Tier 1: google_detour          — re-enter via Google search
       Tier 2: swap_to_alive_proxy    — delete profile, pick alive proxy, recreate
       Tier 3: release_to_pending     — give up, surface to user

  • ProxyDeadError raised by _goto    →  _handle_proxy_dead (Worker)
       Tier 1: switch_to_local_network — keep profile, flip to noproxy
       Tier 2 (only if local also blocked later): swap_to_alive_proxy

Phase 5.3 changes (per user 2026-05-17):
  - Proxy death (ERR_CONNECTION_CLOSED etc.) flips to local network FIRST,
    profile NOT deleted. Only deleted if local also gets blocked.
  - When picking a fresh proxy, test config.PROXIES live and use the first
    alive one. 2 workers sharing the same proxy is now allowed (proxyxoay-style
    rotating proxies don't actually share an IP).
  - If all proxies are dead, sleep 120s and test once more. Still dead →
    fall back to local network.
"""
from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import adspower_helper
from adspower_helper import (
    AdsPower,
    create_profile,
    delete_profile,
    find_group_id,
    list_groups,
    set_local_network,
)
from block_detector import detect_block
from config import ADSPOWER_API, PROXY_ROTATION_KEYS
from proxy_parser import parse_proxy, to_adspower_user_proxy_config
from proxy_rotator import rotate_proxy_ip
from proxy_tester import test_proxy

if TYPE_CHECKING:
    from worker import Worker


GOOGLE_URL = "https://www.google.com/"

# Per user: proxyxoay-style proxies always come back, just slow. Wait this long
# between the first all-dead test and the retry.
PROXY_REVIVE_WAIT_SECONDS = 120


# ─── Tier 1: Google detour ────────────────────────────────────────────────

async def google_detour(scraper, product_id: str, product_url: str) -> bool:
    """Re-enter the product page via Google search so Lowes sees a real
    SERP referrer instead of a direct hit.

    Flow:
      1. goto google.com
      2. type `site:lowes.com/pd "<product_id>"` (productId is globally unique)
      3. submit, wait for results
      4. click the first /pd/ link
      5. detect_block again → True if we now have a clean page

    Returns True if recovery succeeded (page is clean), False otherwise.
    Never raises; logs all failure modes.
    """
    worker_id = scraper.worker_id
    page = scraper.page
    print(f"  [{worker_id}] [recover/tier1] Google detour for {product_id}")
    try:
        # Visit Google. Strip the previously-stuck Referer to look like a
        # fresh search session.
        await page.goto(GOOGLE_URL, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)

        # If Google itself shows a captcha/sorry interstitial, bail early.
        title = (await page.title()).lower()
        if "sorry" in title or "unusual traffic" in title:
            print(f"  [{worker_id}] [recover/tier1] Google captcha — abort detour")
            return False

        # Search box selector — Google occasionally A/B tests, try a few.
        box = page.locator(
            'textarea[name="q"], input[name="q"], textarea[aria-label="Search"]'
        ).first
        await box.wait_for(state="visible", timeout=8000)
        await box.click()
        await box.fill(f'site:lowes.com/pd "{product_id}"')
        await box.press("Enter")

        # Wait for SERP — at least one /pd/ link must appear.
        try:
            await page.wait_for_selector('a[href*="lowes.com/pd/"]', timeout=12000)
        except Exception:
            print(f"  [{worker_id}] [recover/tier1] No Lowes /pd/ result on SERP")
            return False

        # Click the first organic /pd/ link that contains our productId.
        target_link = page.locator(
            f'a[href*="/pd/"][href*="{product_id}"]'
        ).first
        if await target_link.count() == 0:
            # Fallback: first Lowes /pd/ link, even if productId mismatch
            target_link = page.locator('a[href*="lowes.com/pd/"]').first
        try:
            await target_link.wait_for(state="visible", timeout=5000)
        except Exception:
            print(f"  [{worker_id}] [recover/tier1] Lowes link not clickable")
            return False

        async with page.expect_navigation(wait_until="domcontentloaded", timeout=25000):
            await target_link.click()
        await asyncio.sleep(3)

        # Re-check: is the page clean now?
        verdict = await detect_block(page)
        if verdict["kind"] == "ok":
            # Stash a Lowes-side referer so subsequent /pd/ visits in this
            # session look like onward navigation, not direct hits.
            scraper._referer = GOOGLE_URL  # we came from Google
            print(f"  [{worker_id}] [recover/tier1] Google detour succeeded")
            return True
        print(
            f"  [{worker_id}] [recover/tier1] Detour landed on another block: "
            f"{verdict['kind']} — {verdict['detail']}"
        )
        return False
    except Exception as e:
        print(f"  [{worker_id}] [recover/tier1] error: {e}")
        return False


# ─── Proxy-pool health helpers ────────────────────────────────────────────

def _test_one(raw: str) -> bool:
    """Synchronous proxy aliveness test. Returns True if proxy answers."""
    if not raw or not raw.strip():
        return False
    try:
        p = parse_proxy(raw)
    except Exception:
        return False
    try:
        return bool(test_proxy(p).get("ok"))
    except Exception:
        return False


def find_alive_proxy(
    proxies: list[str],
    wait_then_retry: bool = True,
    wait_seconds: int = PROXY_REVIVE_WAIT_SECONDS,
) -> tuple[int, str] | None:
    """Return (index, raw) of first alive proxy in the list, or None.

    If wait_then_retry and the first pass finds none alive, sleeps
    `wait_seconds` then runs the pass exactly ONCE more. Per user: proxies
    "luôn luôn sống chỉ là đôi khi xoay" — they just need time to come back.

    Synchronous (calls requests.get under the hood). Wrap with asyncio.to_thread
    when calling from async code.
    """
    def _scan() -> tuple[int, str] | None:
        for i, raw in enumerate(proxies):
            if _test_one(raw):
                return (i, raw)
        return None

    hit = _scan()
    if hit is not None:
        return hit
    if not wait_then_retry:
        return None
    print(
        f"  [proxy-check] No proxy alive on first pass — "
        f"waiting {wait_seconds}s for rotation, then trying once more..."
    )
    time.sleep(wait_seconds)
    return _scan()


# ─── Tier 1 (proxy-dead): switch to local network, keep profile ───────────

async def switch_to_local_network(worker: "Worker") -> bool:
    """Flip the worker's profile to noproxy (local IP) without deleting it.

    Triggered by ProxyDeadError. Profile is preserved (fingerprint + cookies
    intact) — only the egress changes. Worker keeps scraping until either it
    succeeds or hits a real block, at which point swap_to_alive_proxy runs.

    Returns True on success, False if we couldn't reconnect.
    """
    worker_id = worker.worker_id
    pid = worker.profile_id
    print(f"  [{worker_id}] [recover/proxy-dead] profile {pid} → local network")

    try:
        if worker.scraper:
            await worker.scraper.close()
    except Exception as e:
        print(f"  [{worker_id}] [recover/proxy-dead] close scraper failed (continuing): {e}")
    try:
        await asyncio.to_thread(AdsPower(ADSPOWER_API, pid).stop)
    except Exception as e:
        print(f"  [{worker_id}] [recover/proxy-dead] AdsPower stop failed (continuing): {e}")
    await asyncio.sleep(1.5)

    try:
        await asyncio.to_thread(set_local_network, ADSPOWER_API, pid)
        print(f"  [{worker_id}] [recover/proxy-dead] AdsPower proxy_type=noproxy applied")
    except Exception as e:
        print(f"  [{worker_id}] [recover/proxy-dead] set_local_network failed: {e}")
        return False
    await asyncio.sleep(1.5)

    try:
        await worker.connect()
        if worker.scraper is not None:
            worker.scraper._warmed_up = False
            worker.scraper._proxy_error_count = 0
        worker.using_local = True
        worker.egress_ip = "local"
    except Exception as e:
        print(f"  [{worker_id}] [recover/proxy-dead] reconnect failed: {e}")
        return False

    print(f"  [{worker_id}] [recover/proxy-dead] worker now on local network — resuming")
    return True


# ─── Tier 2: profile swap (delete + pick alive proxy) ─────────────────────

async def swap_to_alive_proxy(worker: "Worker") -> bool:
    """Burn the worker's profile and recreate it with the first alive proxy
    from config.PROXIES (or local network if all are dead).

    Called from two places:
      • Akamai/CF block detected (legacy tier 2)
      • Block detected while worker.using_local=True (Phase 5.3)

    Sharing a proxy with a sibling worker is allowed — proxyxoay rotating
    proxies don't actually share an egress IP between callers.

    Returns True if a new profile was created + connected, False on any
    irrecoverable failure.
    """
    worker_id = worker.worker_id
    old_pid = worker.profile_id
    print(f"  [{worker_id}] [recover/swap-proxy] burning profile {old_pid}, finding alive proxy")

    # 1) Close current scraper / browser
    try:
        if worker.scraper:
            await worker.scraper.close()
    except Exception as e:
        print(f"  [{worker_id}] [recover/swap-proxy] close scraper failed (continuing): {e}")
    try:
        await asyncio.to_thread(AdsPower(ADSPOWER_API, old_pid).stop)
    except Exception as e:
        print(f"  [{worker_id}] [recover/swap-proxy] stop failed (continuing): {e}")
    await asyncio.sleep(1.5)

    # 2) Delete burned profile. Non-fatal: we are abandoning this profile no
    #    matter what. delete_profile is idempotent on an already-missing profile
    #    (returns cleanly), but even a genuine delete error must NOT stop us from
    #    building a fresh working profile — otherwise a vanished/broken profile
    #    would strand the worker in a "profile not exists" loop (the exact bug
    #    the user hit). Press on to create regardless.
    try:
        await asyncio.to_thread(delete_profile, ADSPOWER_API, old_pid)
        print(f"  [{worker_id}] [recover/swap-proxy] deleted {old_pid}")
    except Exception as e:
        print(
            f"  [{worker_id}] [recover/swap-proxy] DELETE failed (continuing to "
            f"recreate anyway): {e}"
        )
    await asyncio.sleep(1.5)

    # 3) Find alive proxy (with 120s revive wait on first all-dead)
    hit = await asyncio.to_thread(find_alive_proxy, worker.all_proxies, True)

    if hit is not None:
        new_idx, new_proxy_raw = hit
        try:
            proxy = parse_proxy(new_proxy_raw)
            proxy_cfg = to_adspower_user_proxy_config(proxy)
        except Exception as e:
            print(f"  [{worker_id}] [recover/swap-proxy] proxy parse failed: {e}")
            return False
        worker.proxy_index = new_idx
        worker.using_local = False
        proxy_label = f"proxy slot [{new_idx}]"
        worker.egress_ip = None  # will be set on first test if needed
    else:
        # All proxies still dead even after the 120s wait — fall back to local.
        print(
            f"  [{worker_id}] [recover/swap-proxy] all proxies still dead after wait — "
            f"creating new profile on local network"
        )
        # AdsPower no-proxy payload: see adspower_helper.set_local_network.
        proxy_cfg = {"proxy_soft": "no_proxy"}
        worker.using_local = True
        proxy_label = "local network"
        worker.egress_ip = "local"

    # 4) Create the new profile
    try:
        gid = await asyncio.to_thread(find_group_id, ADSPOWER_API, "Scraper")
        if not gid:
            groups = await asyncio.to_thread(list_groups, ADSPOWER_API)
            gid = str(groups[0]["group_id"]) if groups else None
        slot_for_os = worker.proxy_index if worker.proxy_index >= 0 else 0
        os_type = ("windows", "mac", "linux")[slot_for_os % 3]
        name = f"lowes-{worker_id}-swap-{int(time.time())}"
        new_pid = await asyncio.to_thread(
            create_profile,
            ADSPOWER_API,
            name=name,
            proxy_config=proxy_cfg,
            group_id=gid,
            os_type=os_type,
        )
        print(
            f"  [{worker_id}] [recover/swap-proxy] created {new_pid} on {proxy_label} (os={os_type})"
        )
    except Exception as e:
        print(f"  [{worker_id}] [recover/swap-proxy] create_profile failed: {e}")
        return False

    # 5) Re-bind worker + reconnect
    worker.profile_id = new_pid
    try:
        await worker.connect()
        if worker.scraper is not None:
            worker.scraper._warmed_up = False
            worker.scraper._proxy_error_count = 0
    except Exception as e:
        print(f"  [{worker_id}] [recover/swap-proxy] reconnect failed: {e}")
        return False

    print(f"  [{worker_id}] [recover/swap-proxy] worker ready on {proxy_label}")
    return True


# ─── Tier 2 (access-denied): rotate-and-recreate ──────────────────────────

async def rotate_and_recreate(worker: "Worker") -> bool:
    """Burn the profile, rotate the proxy's IP from the provider, recreate.

    Phase 5.4 — Access-Denied recovery. Per user instruction:
      "khi bị Access Denied lập tức xóa profile, tạo profile mới sau đó dùng
       API key để đổi proxy từ nhà cung cấp"

    Flow:
      1. Close scraper + stop browser
      2. DELETE profile (must succeed)
      3. Call proxyxoay rotation API for this worker's proxy slot (with
         cooldown wait + 1 retry if HTTP 400 "wait N seconds")
      4. Poll for new egress IP (up to 30s)
      5. CREATE fresh profile bound to the (newly-rotated) same proxy slot
      6. Re-bind + reconnect

    Returns True on success, False if anything irrecoverable failed (caller
    should escalate to swap_to_alive_proxy or release).
    """
    worker_id = worker.worker_id
    old_pid = worker.profile_id
    slot = worker.proxy_index

    if slot < 0 or slot >= len(worker.all_proxies):
        print(
            f"  [{worker_id}] [recover/rotate] worker has no valid proxy slot "
            f"(idx={slot}) — cannot rotate"
        )
        return False

    proxy_raw = worker.all_proxies[slot]
    if not proxy_raw or not proxy_raw.strip():
        print(f"  [{worker_id}] [recover/rotate] proxy slot[{slot}] is empty — cannot rotate")
        return False

    # Map slot → rotation key (config order matches PROXIES order).
    api_key = (
        PROXY_ROTATION_KEYS[slot]
        if slot < len(PROXY_ROTATION_KEYS) else None
    )
    if not api_key:
        print(
            f"  [{worker_id}] [recover/rotate] no PROXY_ROTATION_KEYS[{slot}] — "
            f"falling back to swap_to_alive_proxy"
        )
        return False

    print(f"  [{worker_id}] [recover/rotate] AD recovery — burning {old_pid}, rotating slot[{slot}]")

    # 1) Close current scraper / browser
    try:
        if worker.scraper:
            await worker.scraper.close()
    except Exception as e:
        print(f"  [{worker_id}] [recover/rotate] close scraper failed (continuing): {e}")
    try:
        await asyncio.to_thread(AdsPower(ADSPOWER_API, old_pid).stop)
    except Exception as e:
        print(f"  [{worker_id}] [recover/rotate] AdsPower stop failed (continuing): {e}")
    await asyncio.sleep(1.5)

    # 2) Delete the burned profile. Non-fatal (idempotent on missing profile):
    #    a delete failure must not abort recovery — we still rotate the IP and
    #    create a fresh profile so the worker recovers instead of looping.
    try:
        await asyncio.to_thread(delete_profile, ADSPOWER_API, old_pid)
        print(f"  [{worker_id}] [recover/rotate] deleted {old_pid}")
    except Exception as e:
        print(
            f"  [{worker_id}] [recover/rotate] DELETE failed (continuing to "
            f"rotate + recreate anyway): {e}"
        )
    await asyncio.sleep(1.0)

    # 3) Rotate the proxy IP via provider API (cooldown-aware, polls new IP)
    rot = await asyncio.to_thread(
        rotate_proxy_ip,
        api_key,
        proxy_raw,
        log_prefix=f"  [{worker_id}] [recover/rotate]",
    )
    if not rot.get("ok"):
        # Cooldown too long or transport error — let caller fall back to swap.
        print(
            f"  [{worker_id}] [recover/rotate] rotation failed: {rot.get('message')} "
            f"— caller may fall back"
        )
        return False
    new_ip = rot.get("new_ip") or rot.get("old_ip")

    # 4) Create fresh profile bound to the same proxy slot (now rotated)
    try:
        proxy = parse_proxy(proxy_raw)
        proxy_cfg = to_adspower_user_proxy_config(proxy)
        gid = await asyncio.to_thread(find_group_id, ADSPOWER_API, "Scraper")
        if not gid:
            groups = await asyncio.to_thread(list_groups, ADSPOWER_API)
            gid = str(groups[0]["group_id"]) if groups else None
        os_type = ("windows", "mac", "linux")[slot % 3]
        name = f"lowes-{worker_id}-rotated-{int(time.time())}"
        new_pid = await asyncio.to_thread(
            create_profile,
            ADSPOWER_API,
            name=name,
            proxy_config=proxy_cfg,
            group_id=gid,
            os_type=os_type,
        )
        print(
            f"  [{worker_id}] [recover/rotate] created {new_pid} on slot[{slot}] "
            f"(rotated IP={new_ip}, os={os_type})"
        )
    except Exception as e:
        print(f"  [{worker_id}] [recover/rotate] create_profile failed: {e}")
        return False

    # 5) Re-bind worker + reconnect
    worker.profile_id = new_pid
    worker.using_local = False
    worker.egress_ip = new_ip
    try:
        await worker.connect()
        if worker.scraper is not None:
            worker.scraper._warmed_up = False
            worker.scraper._proxy_error_count = 0
    except Exception as e:
        print(f"  [{worker_id}] [recover/rotate] reconnect failed: {e}")
        return False

    print(f"  [{worker_id}] [recover/rotate] worker ready on slot[{slot}] with IP {new_ip}")
    return True


# Back-compat alias — some older callers still import swap_worker_profile.
swap_worker_profile = swap_to_alive_proxy


# ─── Tier 3: release item back to pending ─────────────────────────────────

def release_to_pending(state, product_id: str, reason: str) -> None:
    """Drop the worker's claim on this item without marking outcome.

    Item goes back to `pending` with a fresh attempts counter (preserves it
    from the cap-fail mechanism but doesn't immediately trigger another
    refill cycle since attempts isn't reset). User can re-arm via UI later
    if they want to retry.

    Side-effect: keeps `last_error` populated so the Dashboard reflects what
    happened.
    """
    import time as _t
    now = _t.time()
    with state._txn() as conn:  # noqa: SLF001
        conn.execute(
            """
            UPDATE jobs
            SET status='pending',
                worker_id=NULL,
                claimed_at=NULL,
                last_error=?,
                updated_at=?
            WHERE product_id=?
            """,
            (reason[:500], now, str(product_id)),
        )
