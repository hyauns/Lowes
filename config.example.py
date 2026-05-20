# Lowes Scraper Configuration — TEMPLATE
#
# Copy this file to `config.py` and fill in your own values. `config.py`
# is gitignored so your secrets stay local.
#
#   Windows PowerShell:  Copy-Item config.example.py config.py
#   Linux / macOS:       cp config.example.py config.py
#
# Then edit `config.py` and replace every "REPLACE_ME_*" placeholder.

# ─── AdsPower ────────────────────────────────────────────────────────────
ADSPOWER_API = "http://127.0.0.1:50325"

# Legacy single-profile (still used by old CLI commands). Set to first of PROFILES if empty.
# Get this from AdsPower → your profile → "ID" column.
ADSPOWER_PROFILE_ID = "REPLACE_ME_ADSPOWER_PROFILE_ID"

# Multi-worker profile pool (Phase 3+). Each index N pairs with PROXIES[N] (1-1).
# Leave entry as "" to auto-create a new profile on first run.
PROFILES = [
    "",   # worker 0 — empty = auto-create on first run
    "",   # worker 1
    "",   # worker 2
]

# Proxy pool, 1-1 with PROFILES. Supported schemes: socks5://, http://, https://
# Accepted formats:
#   - http://user:pass@host:port           (standard URL)
#   - http://host:port@user:pass           (swapped, common with VN proxy sellers)
#   - host:port:user:pass                  (colon-separated, no scheme)
# Leave entry as "" only if you accept profile running on local IP (NOT recommended).
PROXIES = [
    "REPLACE_ME_PROXY_URL_WORKER_0",
    "REPLACE_ME_PROXY_URL_WORKER_1",
    "REPLACE_ME_PROXY_URL_WORKER_2",
]

# Phase 5.4: proxyxoay rotation API keys, 1-1 with PROXIES above.
# When a worker hits Access Denied, recovery deletes the profile and calls
#   GET https://proxyxoay.net/api/rotating-proxy/change-key-ip/<key>
# to rotate the proxy's egress IP from the provider side, then creates a
# fresh profile bound to the (newly-rotated) proxy.
# Leave a key empty ("") if that slot's proxy doesn't support rotation.
PROXY_ROTATION_KEYS = [
    "REPLACE_ME_ROTATION_KEY_WORKER_0",
    "REPLACE_ME_ROTATION_KEY_WORKER_1",
    "REPLACE_ME_ROTATION_KEY_WORKER_2",
]
PROXY_ROTATION_API_BASE = "https://proxyxoay.net/api/rotating-proxy/change-key-ip"
# Max seconds we'll wait when the provider returns HTTP 400 "Vui lòng chờ sau N giây".
# If cooldown > this, recovery escalates to swap_to_alive_proxy.
PROXY_ROTATION_MAX_WAIT = 120
# After rotation API accepts, wait until the proxy is actually ALIVE before
# creating the new profile. Probe every PROXY_ALIVE_WAIT_INTERVAL seconds,
# up to PROXY_ALIVE_WAIT_MAX_ATTEMPTS times.
PROXY_ALIVE_WAIT_INTERVAL = 60
PROXY_ALIVE_WAIT_MAX_ATTEMPTS = 30  # 30 × 60s = 30 minutes absolute cap
# Legacy aliases — kept to satisfy any old imports. Not actively used now.
PROXY_ROTATION_POLL_TIMEOUT = PROXY_ALIVE_WAIT_INTERVAL * PROXY_ALIVE_WAIT_MAX_ATTEMPTS
PROXY_ROTATION_POLL_INTERVAL = PROXY_ALIVE_WAIT_INTERVAL

# Number of workers to spawn. Must be <= len(PROFILES) and <= len(PROXIES).
WORKER_COUNT = 3  # Phase 4: multi-worker. Set to 1 to use legacy single-worker path.

# ─── Listing scrape — dedicated profile + proxy (Phase 5.6) ──────────────
# The `list` action collects product URLs only; doesn't benefit from
# multi-worker. Use a SEPARATE AdsPower profile + proxy so the detail-scrape
# worker pool (PROFILES/PROXIES above) stays untouched while listing runs.
#
# Behavior:
#   • LISTING_PROXY non-empty       → use these values directly for `list`
#   • LISTING_PROXY empty           → fall back to auto-picking alive slot from PROXIES
#   • LISTING_PROFILE_ID empty      → auto-create a fresh profile bound to LISTING_PROXY
#   • LISTING_ROTATION_KEY non-empty → enables proxyxoay rotation for this proxy
#
# Other single-worker actions (`discover`, `setzip`) keep using the alive-slot
# picker from the worker pool — not affected by this section.
LISTING_PROFILE_ID = ""        # empty → auto-create
LISTING_PROXY = ""             # empty → fall back to alive slot from PROXIES
LISTING_ROTATION_KEY = ""      # empty → no rotation for listing proxy

# ─── Lowes ───────────────────────────────────────────────────────────────
BASE_URL = "https://www.lowes.com"
ZIP_CODE = "10001"

# ─── Delays (seconds) ────────────────────────────────────────────────────
MIN_DELAY = 3
MAX_DELAY = 8
CATEGORY_DELAY = 15
TIMEOUT = 60000  # ms

# ─── Directories ─────────────────────────────────────────────────────────
DATA_DIR = "data"
LISTINGS_DIR = "data/listings"
DETAILS_DIR = "data/details"
DEBUG_DIR = "data/debug"

# ─── State manager (Phase 1) ─────────────────────────────────────────────
STATE_DB_PATH = "data/state.db"
STATE_DB_BACKUP = "data/state.db.bak"
# How long a claimed job can stay 'claimed' before another worker can steal it.
# Prevents stuck jobs if a worker crashes without releasing.
CLAIM_STALE_SECONDS = 300  # 5 minutes
# Retry attempts when SQLite reports BUSY
SQLITE_BUSY_RETRIES = 5

# ─── Completeness rules (Phase 2) ────────────────────────────────────────
# A detail is considered COMPLETE only if all required fields are present and non-empty.
# Missing required fields => job re-enqueued as 'needs_refill'.
REQUIRED_DETAIL_FIELDS = {
    # Critical for "done" — missing any of these → error.
    "title": "non_empty_str",
    "price": "non_empty_str",
    "description": "non_empty_str",
    "specifications": "dict_min_1",  # at least 1 spec entry
}
# Images are NOT required. When missing, scrape_detail still saves the data but
# uses the `_missimage` filename suffix so the user knows to add images later.
# Brand and modelNumber are NOT required either — usually extracted, but if a
# page doesn't surface them we still keep the item.

# Max times an item can be scraped and still come back partial.
# After this, it's marked `failed` instead of `needs_refill` so we don't loop
# forever on a page that's permanently soft-blocked or genuinely missing fields.
# User can re-arm via UI "Retry Failed" button (resets attempts → 0).
MAX_REFILL_ATTEMPTS = 3

# ─── Listing filter (REMOVED in Phase 5.6) ───────────────────────────────
# Price filtering is now done via the Lowes URL itself (rangeFacet=
# price_td__<min>__<max> in the listing URL). The in-code filter is gone
# because (a) Lowes' server-side filter is faster and more accurate, and
# (b) keeping two filters in sync is brittle. Pass the filter in the URL:
#   https://www.lowes.com/pl/.../<catId>?rangeFacet=price_td__100__2000
