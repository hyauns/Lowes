"""AdsPower browser helper - start/stop profiles and manage profiles/proxies.

All HTTP calls have explicit timeouts so a hung/missing AdsPower instance can't
block the caller's event loop indefinitely.

In Phase 3 we add profile management:
  - list_profiles / find_profile
  - create_profile (with proxy)
  - update_proxy
  - list_groups (used to choose where to put new profiles)

The class-level instance still binds to one profile (self.pid). The management
helpers are also exposed as module-level functions where it's natural to call
them without a bound profile.

Rate limiting (Phase 5.3 hotfix):
  AdsPower's local API caps at ~1 request/second. Bursts get back
  "Too many request per second, please check". A module-level throttle in
  _request() forces ≥1.1s between any two AdsPower calls, and one-shot retries
  on rate-limit responses. All HTTP in this module goes through _request.
"""
import threading
import time

import requests as req

# Default timeouts. start() can take ~10s for a cold browser; keep 30s ceiling.
DEFAULT_TIMEOUT = 30
QUICK_TIMEOUT = 5

# AdsPower rate-limit: ~1 req/sec. Use 1.1s for safety margin.
_ADS_MIN_INTERVAL = 1.1
_ADS_RATE_LIMIT_RETRY_WAIT = 2.0
_throttle_lock = threading.Lock()
_last_call_ts: float = 0.0


def _throttle() -> None:
    """Block until ≥_ADS_MIN_INTERVAL seconds have passed since the last call.

    Thread-safe: serializes all AdsPower API access across threads (orchestrator
    spawns worker threads that may all reach for the API concurrently during
    recovery flows).
    """
    global _last_call_ts
    with _throttle_lock:
        wait = _ADS_MIN_INTERVAL - (time.time() - _last_call_ts)
        if wait > 0:
            time.sleep(wait)
        _last_call_ts = time.time()


def _request(method: str, url: str, **kwargs):
    """Throttled HTTP call to AdsPower, with one retry on rate-limit responses.

    Returns the parsed JSON dict. Does not raise on `code != 0` — callers use
    _check() for that, OR inspect the response directly (e.g. find_profile
    treats code!=0 as "not found").
    """
    _throttle()
    r = req.request(method, url, **kwargs)
    j = r.json()
    # Defensive: even with throttle, a sibling caller (or a clock skew) might
    # still trigger the rate limit. Retry ONCE after a short wait.
    msg = (j.get("msg") or "").lower()
    if j.get("code") != 0 and "too many request" in msg:
        time.sleep(_ADS_RATE_LIMIT_RETRY_WAIT)
        _throttle()
        r = req.request(method, url, **kwargs)
        j = r.json()
    return j


# ─── Module-level management API ─────────────────────────────────────────

def _check(resp_json: dict, context: str) -> dict:
    if resp_json.get("code") != 0:
        raise RuntimeError(f"AdsPower {context} failed: {resp_json.get('msg')}")
    return resp_json.get("data", {}) or {}


def list_groups(api_url: str) -> list[dict]:
    j = _request(
        "GET",
        f"{api_url.rstrip('/')}/api/v1/group/list?page_size=100",
        timeout=QUICK_TIMEOUT,
    )
    data = _check(j, "list_groups")
    return data.get("list", []) or []


def find_group_id(api_url: str, name: str) -> str | None:
    for g in list_groups(api_url):
        if g.get("group_name") == name:
            return str(g.get("group_id"))
    return None


def find_profile(api_url: str, user_id: str) -> dict | None:
    """Return the profile dict, or None if not found."""
    if not user_id:
        return None
    j = _request(
        "GET",
        f"{api_url.rstrip('/')}/api/v1/user/list?user_id={user_id}",
        timeout=QUICK_TIMEOUT,
    )
    if j.get("code") != 0:
        return None
    items = (j.get("data") or {}).get("list") or []
    for it in items:
        if str(it.get("user_id")) == str(user_id):
            return it
    return None


def delete_profile(api_url: str, user_id: str) -> None:
    """Delete an AdsPower profile permanently.

    Used by Phase 5.2 (Access Denied tier 2): when a profile is burned by
    Akamai, we tear it down completely before creating a fresh one. The
    caller must have stopped the browser first.

    AdsPower v1 takes `user_ids` (list) in JSON body. Raises RuntimeError
    if the API reports failure — Phase 5.2 needs deletion to definitively
    succeed before swapping, per explicit user instruction.
    """
    if not user_id:
        raise ValueError("delete_profile: user_id is required")
    payload = {"user_ids": [str(user_id)]}
    j = _request(
        "POST",
        f"{api_url.rstrip('/')}/api/v1/user/delete",
        json=payload,
        timeout=DEFAULT_TIMEOUT,
    )
    _check(j, f"delete_profile({user_id})")


def update_proxy(api_url: str, user_id: str, proxy_config: dict) -> None:
    """Update only the proxy fields of an existing profile."""
    payload = {"user_id": user_id, "user_proxy_config": proxy_config}
    j = _request(
        "POST",
        f"{api_url.rstrip('/')}/api/v1/user/update",
        json=payload,
        timeout=DEFAULT_TIMEOUT,
    )
    _check(j, f"update_proxy({user_id})")


def set_local_network(api_url: str, user_id: str) -> None:
    """Switch a profile to use the machine's local network (no proxy).

    Phase 5.3 fallback: when a worker's proxy goes dead (ERR_CONNECTION_CLOSED
    etc.), we stop the browser, flip the profile's proxy to no-proxy, and
    restart — instead of deleting the profile. The profile keeps its
    fingerprint + cookies, only the egress changes to the local IP.

    Per AdsPower docs, the no-proxy payload is {"proxy_soft": "no_proxy"}.
    Note the underscore: proxy_soft = "no_proxy" (provider id), NOT
    proxy_type = "noproxy" (that shape is rejected by /user/create with
    "user_proxy_config incorrect format").
    """
    update_proxy(api_url, user_id, {"proxy_soft": "no_proxy"})


# Desktop OS only — Lowes is a US desktop e-commerce site, and our scraper
# selectors target the desktop layout. Mobile fingerprints (iOS/Android) make
# Lowes serve a different DOM and break the scraper. Hard rule.
ALLOWED_OS = ("windows", "mac", "linux")
DEFAULT_PROFILE_OS = "windows"

# Constrain AdsPower's UA randomization to desktop systems only. Earlier we
# tried pinning an explicit `ua` with Chrome 131, but that made AdsPower try
# to download kernel 131 ("SunBrowser 131 is updating, waiting for download")
# and refuse to start the browser. By NOT setting `ua` and instead constraining
# `random_ua.ua_system_version`, AdsPower picks a desktop UA whose Chrome major
# version matches a kernel it already has installed — no surprise downloads.
#
# AdsPower docs require random_ua to be a JSON OBJECT, not a string.
_DESKTOP_RANDOM_UA_SYSTEM = {
    "windows": "Windows 10",
    "mac": "Mac OS X 14",
    "linux": "Linux",
}


def create_profile(
    api_url: str,
    name: str,
    proxy_config: dict,
    group_id: str | None = None,
    fingerprint_overrides: dict | None = None,
    os_type: str = DEFAULT_PROFILE_OS,
) -> str:
    """Create a new profile and return its assigned user_id.

    group_id defaults to the 'Scraper' group if it exists, else the first group.

    os_type must be one of ALLOWED_OS (windows / mac / linux). We never create
    mobile profiles — see comment above.
    """
    api_url = api_url.rstrip("/")
    if group_id is None:
        groups = list_groups(api_url)
        if not groups:
            raise RuntimeError("AdsPower has no groups — create one in the UI first")
        gid = find_group_id(api_url, "Scraper")
        group_id = gid or str(groups[0]["group_id"])

    os_type = (os_type or DEFAULT_PROFILE_OS).lower()
    if os_type not in ALLOWED_OS:
        raise ValueError(
            f"os_type={os_type!r} is not allowed for this scraper. "
            f"Must be one of {ALLOWED_OS} (no mobile fingerprints)."
        )

    # Constrain UA randomization to desktop. No explicit `ua` — pinning one
    # forced AdsPower to download a matching kernel version on the fly, which
    # crashed startup ("SunBrowser X is updating, waiting for download").
    # `random_ua.ua_system_version` set to a desktop OS prevents AdsPower from
    # ever rolling a mobile (Android/iOS) UA — the original bug.
    fp: dict = {
        "automatic_timezone": "1",
        "language": ["en-US", "en"],
        "os": os_type,
        "random_ua": {
            "ua_browser": ["chrome"],
            "ua_system_version": [_DESKTOP_RANDOM_UA_SYSTEM[os_type]],
        },
        "browser_kernel_config": {"version": "ua_auto", "type": "chrome"},
    }
    if fingerprint_overrides:
        # Allow caller to override, but block any attempt to set a mobile OS or
        # mobile UA.
        forced_os = fingerprint_overrides.get("os")
        if forced_os and forced_os.lower() not in ALLOWED_OS:
            raise ValueError(
                f"fingerprint_overrides.os={forced_os!r} rejected — "
                f"mobile profiles are not supported."
            )
        forced_ua = fingerprint_overrides.get("ua", "")
        if forced_ua and any(t in forced_ua for t in ("Android", "iPhone", "iPad", "Mobile")):
            raise ValueError(
                f"fingerprint_overrides.ua looks mobile ({forced_ua[:60]}...) — rejected."
            )
        fp.update(fingerprint_overrides)

    payload = {
        "name": name,
        "group_id": group_id,
        "user_proxy_config": proxy_config,
        "fingerprint_config": fp,
    }
    j = _request(
        "POST",
        f"{api_url}/api/v1/user/create",
        json=payload,
        timeout=DEFAULT_TIMEOUT,
    )
    data = _check(j, "create_profile")
    user_id = data.get("id")
    if not user_id:
        raise RuntimeError(f"create_profile: no 'id' in response: {data}")
    return str(user_id)


# ─── Profile-bound class ─────────────────────────────────────────────────

class AdsPower:
    def __init__(self, api_url, profile_id):
        self.api = api_url.rstrip("/")
        self.pid = profile_id

    def start(self):
        """Start browser profile, return puppeteer WS endpoint."""
        try:
            j = _request(
                "GET",
                f"{self.api}/api/v1/browser/start?user_id={self.pid}",
                timeout=DEFAULT_TIMEOUT,
            )
        except req.exceptions.ConnectionError as e:
            raise RuntimeError(
                f"Cannot reach AdsPower at {self.api}. Is AdsPower running? ({e})"
            ) from e
        except req.exceptions.Timeout as e:
            raise RuntimeError(
                f"AdsPower start timed out after {DEFAULT_TIMEOUT}s. "
                f"Profile {self.pid} may be stuck."
            ) from e
        if j.get("code") != 0:
            raise RuntimeError(f"AdsPower start failed: {j.get('msg')}")
        ws = j["data"]["ws"]["puppeteer"]
        print(f"[AdsPower] Started profile={self.pid} WS: {ws[:60]}...")
        return ws

    def stop(self):
        """Stop browser profile."""
        try:
            j = _request(
                "GET",
                f"{self.api}/api/v1/browser/stop?user_id={self.pid}",
                timeout=QUICK_TIMEOUT,
            )
            print(f"[AdsPower] Stop {self.pid}: {j.get('msg', 'ok')}")
        except Exception as e:
            print(f"[AdsPower] Stop {self.pid} error (non-fatal): {e}")

    def is_active(self):
        try:
            j = _request(
                "GET",
                f"{self.api}/api/v1/browser/active?user_id={self.pid}",
                timeout=QUICK_TIMEOUT,
            )
            return j.get("data", {}).get("status") == "Active"
        except Exception:
            return False
