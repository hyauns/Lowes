# Lowes Scraper — Project Context

> Resume document for future Claude sessions. Keep this updated as phases land.

## What this project does

A scraper for **lowes.com** product catalogs. Driven by Playwright through an
**AdsPower** anti-detect browser profile (with proxy), it walks category pages,
extracts product listings, then scrapes per-product detail pages. Data is saved
as JSON files under `data/listings/` and `data/details/`.

The project is being upgraded from a single-worker CLI into a multi-worker
queue-backed system with a local web UI.

**User profile (the human):** Vietnamese speaker, primary language for chat is
Vietnamese. Runs Windows 11 + PowerShell. Python 3.12. Uses AdsPower for
browser sessions. Conversational style — likes incremental, phased delivery
with tests at each step. Prefers technical decisions explained, not just done.

---

## Tech stack

| Layer | Choice |
|---|---|
| Language | Python 3.12 |
| Browser automation | Playwright (async) connected to AdsPower over CDP |
| Anti-detect browser | AdsPower (local API at `http://127.0.0.1:50325`) |
| State / job queue | SQLite (WAL mode, atomic claim, retry-on-busy) |
| Web UI backend | FastAPI + uvicorn (port 8765) |
| Web UI frontend | Vanilla HTML/CSS/JS (no build step), WebSocket for live logs |
| Proxy formats | http, https, socks5 (PySocks needed for socks) |

---

## File map

```
Lowes/
├── scraper.py              # Main scraper class + CLI commands
├── adspower_helper.py      # AdsPower API: start/stop + profile mgmt (Phase 3)
├── state_manager.py        # SQLite job queue, atomic claim, resume support (Phase 1)
├── completeness.py         # check_completeness + merge_detail (Phase 2)
├── proxy_parser.py         # parse 4 proxy formats → canonical dict (Phase 3)
├── proxy_tester.py         # live test proxy via HTTPS echo (Phase 3)
├── profile_setup.py        # bootstrap: parse → test → find/create → bind (Phase 3)
├── app.py                  # FastAPI server + Runner orchestrator (Phase 1B)
├── config.py               # all tunables (profiles, proxies, required fields, etc.)
├── dump_page.py            # standalone HTML dumper (pre-existing utility)
├── viewer.html             # static data viewer (pre-existing, embedded in UI)
├── ui/
│   ├── index.html          # 5-tab SPA: Dashboard / Runner / Queue / Data / Settings
│   ├── style.css           # dark theme, monospace-friendly
│   └── app.js              # vanilla JS, WebSocket auto-reconnect
├── test_phase1.py          # 13 assertions for state manager
├── test_phase2.py          # 22 assertions for completeness + merge
├── data/
│   ├── state.db            # SQLite job queue (created at first run)
│   ├── state.db.bak        # auto-backup at each startup
│   ├── listings/*.json     # one file per category
│   ├── details/*.json      # one file per productId
│   └── debug/              # __NEXT_DATA__ dumps when parser fails
├── requirements.txt
└── CLAUDE.md               # this file
```

---

## Status: phases

### ✅ Phase 1 — SQLite state manager + atomic claim (DONE)
- `state_manager.py` with WAL + busy_timeout + retry loop
- One connection per thread via `threading.local`
- Auto-backup `state.db` → `state.db.bak` at startup, plus `integrity_check`
- Job statuses: `pending → claimed → done | failed | needs_refill`
- Stale-claim recovery (`CLAIM_STALE_SECONDS = 300`)
- Listing scrape auto-enqueues jobs; detail scrape pulls from queue
- Ctrl+C handler releases in-flight claim → clean resume
- CLI: `python scraper.py status [category-url]`
- Tests: 13/13 pass (`python test_phase1.py`)

### ✅ Phase 1B — Web UI (FastAPI + WebSocket) (DONE)
- `python app.py` boots server at http://127.0.0.1:8765, auto-opens browser
- Tabs: Dashboard, Runner, Queue, Data (embeds viewer.html), Settings
- Live log via `LogBroadcaster` + `TeeStream` (intercepts stdout, fans out to WS)
- Single-slot `Runner` with cooperative stop (`asyncio.Event`)
- API: `/api/stats`, `/api/categories`, `/api/jobs`, `/api/job/{pid}`,
  `/api/runner/{start,stop,status}`, `/api/state/{release-stale,reconcile,requeue-refill}`,
  `/ws/logs`
- Note: only **one** scraper task runs at a time in Phase 1B — multi-worker
  scheduling lands in Phase 4 but plugs into the same Runner slot.

### ✅ Phase 2 — Completeness check + gap-fill merge (DONE)
- `check_completeness(detail) → (is_complete, missing_fields)` against
  `REQUIRED_DETAIL_FIELDS` in config.py
- `merge_detail(old, new)`:
  - **Static** fields (title, brand, modelNumber, itemNumber, gtin, sku, mpn,
    description, descriptionBullets, featureText, featureImages):
    old wins if non-empty; new only fills gaps
  - **Dynamic** fields (`price, wasPrice, rating, reviewCount`):
    new wins unless new is empty
  - `specifications` (dict): key-by-key, old wins per key
  - List fields (images, etc.): union + dedup, old order first
  - `scraped_at` always refreshed
- Queue loop classifies each job: `scraped / refilled / partial / skipped / errors`
- New CLI: `python scraper.py reconcile <category-url>` — scan existing detail
  JSONs and reclassify their DB status (useful after a fresh state.db)
- UI: per-row "Reconcile" and "Refresh all" buttons in Dashboard
- Tests: 22/22 pass (`python test_phase2.py`)

### ✅ Phase 3 — Profile + proxy auto-management (DONE)
- `proxy_parser.py` handles 4 input shapes:
  - Standard URL `scheme://user:pass@host:port`
  - Swapped URL `scheme://host:port@user:pass` (user's actual format)
  - Colon-separated `host:port:user:pass`
  - No-auth `scheme://host:port`
- `proxy_tester.py` tests live via api.ipify / ifconfig.me / ipinfo.io
- `adspower_helper.py` extended: `list_groups, find_profile, update_proxy,
  create_profile`
- `profile_setup.py`:
  - `setup_worker_profile(api, idx, profile_id, proxy_raw)` — idempotent
    bootstrap (find or create profile, bind/update proxy)
  - `setup_all_workers(...)` — enforces 1-1 profile:proxy mapping, errors on
    duplicate proxy strings or list-length mismatch
- Runner now bootstraps worker 0 before connecting to browser
- UI Settings tab: worker pool table + per-row Test IP / Setup buttons, plus
  a freeform proxy tester
- Live tested with a swapped-URL proxyxoay rotating-IP proxy
  → egress IP confirmed VN (replace with your own in `config.py`)

### ⏳ Phase 4 — Multi-worker async (NEXT)
Plan:
- `worker.py`: `Worker(worker_id, profile, proxy)` owns one LowesScraper + AdsPower session
- `orchestrator.py`: spawns N workers via `asyncio.gather`, each calls
  `state.claim_next(worker_id, category)` in a loop
- Bootstrap all N profiles up-front via `setup_all_workers`
- SIGINT/UI-stop handler signals all workers; each finishes its current item
  and releases the claim cleanly
- `release_stale()` runs on startup to free claims from crashed workers
- UI: Dashboard "Workers" cards showing live `profile / egress_ip /
  current_product / elapsed`
- Test: 3 workers, verify no duplicate item across workers; kill mid-run,
  resume preserves progress

Test mốc: 3 worker không trùng item, Ctrl+C → DB nhất quán, chạy lại tiếp đúng chỗ.

### ⏳ Phase 5 — Cloudflare detect + pause-and-prompt
Plan:
- `cloudflare.py`:
  - `detect_cf(page)`: title contains "Just a moment", body "Checking your
    browser", or selectors `#challenge-form`, `iframe[src*="challenges.cloudflare.com"]`
  - `wait_passive(page, timeout=30s)`: let the JS challenge auto-pass
  - `prompt_manual_solve(worker_id)`: pause that worker, log clearly which
    AdsPower profile needs intervention, await UI button click (or terminal Enter)
- Worker that's CF-blocked holds its claim (no release) so no other worker
  re-grabs the item
- Other workers keep running normally
- Retry with backoff after CF passes
- UI: red badge on the affected worker card + "Mark solved" button

Decision đã chốt với user: pause-and-prompt (không skip, không stop-all).

### ⏳ Phase 6 — CLI polish + batch mode
Plan:
- `python scraper.py run <category-url> --workers 3 --pages 1-10` for full
  multi-worker pipeline from CLI
- `python scraper.py refill` to chỉ chạy needs_refill items
- Per-worker prefixed log lines in console
- Better progress display

### ⏳ Phase 7 — UI completeness
The UI skeleton from Phase 1B already has placeholders. This phase fills them:
- Dashboard: per-worker live cards (Phase 4 hook)
- Queue: per-row Retry / Refill / View detail buttons
- Settings: edit PROFILES/PROXIES live (write to config.py) + test buttons
  already added in Phase 3
- Inline detail viewer (instead of iframe to viewer.html)

---

## Key decisions (do not re-litigate without user)

| # | Decision | Why |
|---|---|---|
| 1 | **SQLite, not JSON+filelock**, for state | Atomic claim, queryable, fast |
| 2 | **WAL mode + busy_timeout + retry** | Avoid "database malformed" — user specifically asked for this hardening |
| 3 | **1-1 profile:proxy mapping required** | Prevents 2 profiles sharing the same IP; refuses to start otherwise |
| 4 | **Cloudflare fallback = pause + prompt user** | Not skip (loses data), not stop-all (kills good workers) |
| 5 | **Required fields**: title, brand, price, modelNumber, images≥1, specifications≥5 | Strict enough to catch broken scrapes, loose enough that real products pass |
| 6 | **Price/rating are "dynamic"** — always refresh on merge | They change in the real world; everything else is "static" and gets preserved |
| 7 | **Web UI (FastAPI), not Streamlit / PyWebView / Electron** | Lightweight, extensible, native browser |
| 8 | **UI app does NOT block on `input()` fallback** when running | Terminal isn't visible to user from UI; print and continue |

---

## How to run

### First-time setup
```powershell
pip install -r requirements.txt
playwright install chromium     # if not already
# For SOCKS5 proxies only:
# pip install "requests[socks]"
```

Edit `config.py`:
- Set `ADSPOWER_PROFILE_ID` to one of your AdsPower profile IDs (used by legacy CLI)
- Fill `PROFILES[]` and `PROXIES[]` 1-1 (any empty `PROFILES[i]` auto-creates)
- Tune `WORKER_COUNT` (currently 1; bump after Phase 4)
- Make sure AdsPower app is running locally (default API port 50325)

### Web UI (recommended)
```powershell
python app.py
```
Browser auto-opens at http://127.0.0.1:8765.
- **Settings tab** first: click Test IP / Setup on worker 0 to verify
- **Runner tab**: pick action (setzip → list → detail → full), paste category URL, Start
- **Dashboard**: live stats + Reconcile (re-scan existing files) / Refresh all
- **Queue**: filtered job list

### CLI
```powershell
python scraper.py discover                    # find all categories
python scraper.py setzip                      # set delivery zip
python scraper.py list <url> --pages 1-3      # scrape listing pages
python scraper.py detail <url>                # consume detail queue
python scraper.py full <url> --pages 1-3      # listing + detail
python scraper.py status [<url>]              # queue stats
python scraper.py reconcile <url>             # scan files, update DB status
```

### Tests
```powershell
python test_phase1.py                          # SQLite atomic + resume (13 tests)
python test_phase2.py                          # merge + completeness (22 tests)
```

---

## Environment facts (template — fill in your own in `config.py`)

- **AdsPower API** reachable at `http://127.0.0.1:50325` (default local port)
- **AdsPower groups**: create one named "Scraper" (used as default for new profiles)
- **Active profile**: pick any AdsPower profile ID — set `ADSPOWER_PROFILE_ID`
  in `config.py`
- **Proxy format note**: this scraper supports `scheme://user:pass@host:port`
  (standard), `scheme://host:port@user:pass` (swapped — common with VN
  proxyxoay), and `host:port:user:pass` (colon-separated). See
  `proxy_parser.py` for the canonical handling.

---

## Gotchas / lessons from this session

1. **Python doesn't auto-reload modules.** After editing scraper.py / app.py,
   user MUST restart `python app.py` (Ctrl+C, re-run). If a fix doesn't seem
   applied, this is almost always why. The error "Auto zip-set incomplete"
   reappearing after a fix is the canonical symptom.

2. **Sync HTTP calls block the FastAPI event loop.** This caused the original
   "Failed to fetch" bug — `requests.get(adspower)` ran inside the event loop
   and starved the response write. **Always wrap blocking external calls in
   `asyncio.to_thread(...)`** when called from an async path.

3. **AdsPower API calls need timeouts.** `requests.get` defaults to no timeout.
   `adspower_helper.py` uses `DEFAULT_TIMEOUT=30` for `start` and `QUICK_TIMEOUT=5`
   for status/list/update calls.

4. **The Lowes zip widget uses a tooltip popup, NOT a store finder.**
   Selectors that work:
   - Trigger: `[data-testid="update-zipcode-link"]`
   - Input: `input.searchBoxContainer` (also `input[placeholder="Enter Zip Code"]`)
   - Button: `button.updateButton:not([disabled])` (need `:not([disabled])`
     because the button stays disabled until the React form-state has a value;
     type with `delay=80` so listener fires per keystroke)

5. **JS embedded in Python strings emits SyntaxWarning** for `\?` etc.
   Pre-existing in `scraper.py` line ~857 (`src.replace(/\?size=\w+/, '')`).
   Not from our changes; leave alone unless cleaning up the broader file.

6. **`needs_refill` is prioritized over `pending` in `claim_next`** so refills
   complete before new fresh scrapes. See `state_manager.py` ORDER BY clause.

7. **`StateManager._txn()` uses `BEGIN IMMEDIATE`** — write lock acquired
   upfront, avoiding deadlocks between concurrent claim attempts.

---

## How to pick up in the next session

When resuming:

1. Read this file first (it'll be loaded into context automatically).
2. Check git log / file mtimes to see what changed since this was last updated.
3. Run `python test_phase1.py && python test_phase2.py` to verify regressions.
4. Run `python app.py` and click through tabs to verify UI still works.
5. If user starts a new feature, identify which phase it belongs to and
   update this file's "Status" section.
6. Common starting points:
   - "Tiếp tục phase 4" → start with `worker.py` + `orchestrator.py` design
   - "Lỗi X" → look at the Gotchas section, especially #1 (restart) and #2 (block)
   - "Thêm field Y vào detail" → update `REQUIRED_DETAIL_FIELDS` in config.py,
     then run `reconcile` to reclassify existing files

---

## Open questions / TODOs not yet started

- [ ] Phase 4: Multi-worker orchestrator
- [ ] Phase 5: Cloudflare handling
- [ ] Phase 6: CLI polish / batch mode
- [ ] Phase 7: UI completeness (worker cards, inline detail, settings editor)
- [ ] Decide: should `config.py` become editable from UI (write-through)?
  User hinted at this in Phase 7 description.
- [ ] The pre-existing `\?` SyntaxWarning in scraper.py — fix when refactoring
  the detail extraction JS.
