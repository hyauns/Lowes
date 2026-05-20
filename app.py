"""
Lowes Scraper - Web UI App (FastAPI).

Run:
    python app.py

Auto-opens http://127.0.0.1:8765 in the default browser.

Architecture
------------
- FastAPI + WebSocket on a single port (8765).
- One global "runner" slot: only one scraper task at a time in Phase 1B.
  (Multi-worker scheduling lands in Phase 4 and will plug into this same slot.)
- Stdout from the running scraper is intercepted and pushed to all WS clients.
- State queries (jobs, stats) read straight from SQLite via StateManager.
"""
from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import sys
import threading
import time
import webbrowser
from datetime import datetime
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import importlib
import config as cfg
# Static (non-tunable) — fine to bind at import.
from config import (
    ADSPOWER_API,
    ADSPOWER_PROFILE_ID,
    DETAILS_DIR,
    LISTINGS_DIR,
    DATA_DIR,
)
from scraper import LowesScraper, _name_from_url, _parse_pages, _safe_name
from state_manager import StateManager
from profile_setup import setup_worker_profile, setup_all_workers
from proxy_tester import test_proxy_string
from orchestrator import Orchestrator


def reload_config() -> dict:
    """Re-read config.py from disk. Tunable values (PROFILES/PROXIES/WORKER_COUNT)
    are accessed as `cfg.X` so they pick up the new values immediately.

    Returns a small dict echoing the loaded values for the UI to display.
    """
    importlib.reload(cfg)
    return {
        "worker_count": cfg.WORKER_COUNT,
        "profiles": list(cfg.PROFILES),
        "proxies_count": len(cfg.PROXIES),
    }


# ─── Globals ─────────────────────────────────────────────────────────────

UI_DIR = os.path.join(os.path.dirname(__file__), "ui")

app = FastAPI(title="Lowes Scraper UI")


class LogBroadcaster:
    """Captures lines and fans them out to subscribed WebSocket clients.

    Thread-safe: scraper logs come from the asyncio loop, but writes may
    arrive via redirected stdout on the same loop. We keep a bounded ring
    of recent lines so new clients see backlog instantly.
    """

    def __init__(self, max_buffer: int = 500):
        self._clients: set[asyncio.Queue] = set()
        self._lock = threading.Lock()
        self._buffer: list[dict] = []
        self._max = max_buffer

    def _entry(self, line: str, level: str = "info") -> dict:
        return {"t": datetime.now().strftime("%H:%M:%S"), "level": level, "msg": line}

    def push(self, line: str, level: str = "info"):
        line = line.rstrip("\n")
        if not line:
            return
        e = self._entry(line, level)
        with self._lock:
            self._buffer.append(e)
            if len(self._buffer) > self._max:
                self._buffer = self._buffer[-self._max:]
            clients = list(self._clients)
        for q in clients:
            try:
                q.put_nowait(e)
            except asyncio.QueueFull:
                pass  # client too slow; drop

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        with self._lock:
            self._clients.add(q)
            backlog = list(self._buffer)
        for e in backlog:
            try:
                q.put_nowait(e)
            except asyncio.QueueFull:
                break
        return q

    def unsubscribe(self, q: asyncio.Queue):
        with self._lock:
            self._clients.discard(q)


class TeeStream(io.TextIOBase):
    """File-like that writes to the original stream AND to the broadcaster."""

    def __init__(self, original, broadcaster: LogBroadcaster):
        self._original = original
        self._b = broadcaster
        self._lock = threading.Lock()
        self._partial = ""

    def write(self, s: str) -> int:
        with self._lock:
            self._original.write(s)
            try:
                self._original.flush()
            except Exception:
                pass
            buf = self._partial + s
            *lines, self._partial = buf.split("\n")
        for ln in lines:
            self._b.push(ln)
        return len(s)

    def flush(self):
        try:
            self._original.flush()
        except Exception:
            pass


broadcaster = LogBroadcaster()
state = StateManager()


def _pick_alive_slot(proxies: list[str]) -> tuple[int | None, str | None]:
    """Phase 5.5: pick the FIRST proxy slot that is currently alive.

    Tests each proxy via test_proxy (HTTPS echo through the proxy). Returns
    (slot_index, raw_proxy_string) of the first alive one. If all are dead,
    waits 120s once for proxyxoay rotation and re-tests. Returns (None, None)
    if everything is still dead — caller should fall back to local network.

    Synchronous (calls requests under the hood). Wrap with asyncio.to_thread.
    """
    from recovery import find_alive_proxy
    hit = find_alive_proxy(proxies, wait_then_retry=True)
    if hit is None:
        return None, None
    idx, raw = hit
    return idx, raw


def _bootstrap_listing_profile(proxy_raw: str) -> dict:
    """Phase 5.6: bootstrap the dedicated `list`-action profile from config.

    Uses LISTING_PROFILE_ID + the provided proxy. If LISTING_PROFILE_ID is
    empty, AdsPower auto-creates a fresh profile and binds the proxy. If the
    proxy is dead at probe time, falls back to local network (so listing can
    still run on local IP). Returns the setup_worker_profile result dict.

    Synchronous — wrap with asyncio.to_thread when calling from async code.
    """
    listing_profile = (getattr(cfg, "LISTING_PROFILE_ID", "") or "").strip() or None
    return setup_worker_profile(
        ADSPOWER_API,
        worker_index=0,            # cosmetic — used only for name/os rotation
        profile_id=listing_profile,
        proxy_raw=proxy_raw,
        verify_proxy=True,         # confirm the configured listing proxy is alive
        fallback_to_local=True,    # if dead, bind to local network rather than fail
    )


class Runner:
    """Single-slot runner. Only one scraper task active at a time.

    Phase 4: when WORKER_COUNT > 1 and action is `detail` / `full`, the slot
    holds a multi-worker Orchestrator instead of a single LowesScraper.
    """

    def __init__(self):
        self.task: Optional[asyncio.Task] = None
        self.stop_event = asyncio.Event()
        self.action: Optional[str] = None
        self.started_at: Optional[float] = None
        self.scraper: Optional[LowesScraper] = None
        self.orchestrator: Optional[Orchestrator] = None

    def is_running(self) -> bool:
        return self.task is not None and not self.task.done()

    def status(self) -> dict:
        if not self.task:
            return {"running": False}
        return {
            "running": not self.task.done(),
            "action": self.action,
            "started_at": self.started_at,
            "elapsed": (time.time() - self.started_at) if self.started_at else 0,
            "mode": "multi" if self.orchestrator else "single",
        }

    async def start(self, action: str, params: dict):
        if self.is_running():
            raise HTTPException(409, "A scraper task is already running")
        self.stop_event.clear()
        self.action = action
        self.started_at = time.time()
        loop = asyncio.get_running_loop()
        self.task = loop.create_task(self._run(action, params))

    async def stop(self):
        """Cooperative stop. Scraper/orchestrator check stop_event between items.

        We deliberately swallow CancelledError and any task-side exceptions here.
        The caller (HTTP /api/runner/stop) only cares that the task is no longer
        running. The task's own outcome is already streamed via the WS log.
        """
        if not self.is_running():
            return
        self.stop_event.set()
        if self.orchestrator:
            self.orchestrator.request_stop()
        # Cancel only after a short grace period (multi-worker may need longer
        # because a worker can be mid-Playwright-timeout, up to 30s per accordion).
        grace = 60 if self.orchestrator else 15
        try:
            try:
                await asyncio.wait_for(asyncio.shield(self.task), timeout=grace)
            except asyncio.TimeoutError:
                self.task.cancel()
                with contextlib.suppress(BaseException):
                    await self.task
        except BaseException as e:
            # CancelledError, task exceptions, etc. — already logged inside the task.
            # The endpoint must NOT propagate this, otherwise FastAPI returns 500.
            print(f"[Runner] stop(): swallowed task exit signal: {type(e).__name__}")

    async def _run(self, action: str, params: dict):
        """Dispatch the requested action, capturing stdout to the WS log."""
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = TeeStream(old_stdout, broadcaster)
        sys.stderr = TeeStream(old_stderr, broadcaster)
        try:
            broadcaster.push(f"=== [{action}] starting ===", "info")

            # Always read tunables fresh from disk in case user edited config.py
            # between runs (no need to restart `python app.py` for proxy/worker tweaks).
            reload_config()
            broadcaster.push(
                f"[Config] loaded WORKER_COUNT={cfg.WORKER_COUNT}, "
                f"PROXIES={len(cfg.PROXIES)} entries",
                "info",
            )

            # Multi-worker dispatch: detail / full → orchestrator when there's
            # more than one worker to use. If the user explicitly requested
            # workers=1 for this run, we still take the multi path (because the
            # auto-cap + warmup-per-worker flow lives there). Single path is
            # reserved for actions that never benefit from parallelism.
            requested_workers = params.get("workers")
            use_multi = action in ("detail", "full") and (
                (requested_workers and requested_workers > 0) or cfg.WORKER_COUNT > 1
            )

            # Batch-by-category-name (2026-05-20): UI dropdown sends
            # `category_names` for detail action. The dispatcher loops over
            # them sequentially, passing the name (not URL) to the handler.
            # This bypasses the URL field entirely — useful when the user
            # has many categories queued up and wants to pick which ones to
            # process without re-pasting URLs.
            cat_names = params.pop("category_names", None)
            if cat_names and action == "detail":
                broadcaster.push(
                    f"[Multi-Cat] batch run: {len(cat_names)} categories from queue, "
                    f"will process sequentially",
                    "info",
                )
                for idx, one_cat in enumerate(cat_names, start=1):
                    if self.stop_event.is_set():
                        broadcaster.push(
                            f"[Multi-Cat] Stop requested — aborting after "
                            f"{idx - 1}/{len(cat_names)} categories",
                            "warn",
                        )
                        break
                    broadcaster.push(
                        f"=== [Multi-Cat {idx}/{len(cat_names)}] '{one_cat}' ===",
                        "info",
                    )
                    per_params = {k: v for k, v in params.items() if k != "url"}
                    per_params["category_name"] = one_cat
                    try:
                        if use_multi:
                            await self._run_multi(action, per_params)
                        else:
                            await self._run_single(action, per_params)
                    except Exception as inner_e:
                        broadcaster.push(
                            f"!!! [Multi-Cat {idx}/{len(cat_names)}] '{one_cat}': "
                            f"{type(inner_e).__name__}: {inner_e}",
                            "error",
                        )
                        import traceback
                        broadcaster.push(traceback.format_exc(), "error")
                else:
                    broadcaster.push(
                        f"[Multi-Cat] all {len(cat_names)} categories processed",
                        "info",
                    )
                # Skip the urls / single-path branches below.
                broadcaster.push(f"=== [{action}] finished ===", "info")
                return

            # Batch-URL support (2026-05-18): /api/runner/start can pass a
            # `urls` list. The dispatcher loops over them in order, calling
            # the SAME single/multi handler per URL with `params["url"]`
            # set to the current one. Between iterations we honour
            # stop_event so the user can halt mid-batch.
            urls = params.pop("urls", None)
            if urls and action in ("list", "detail", "full") and len(urls) > 1:
                broadcaster.push(
                    f"[Multi-URL] batch run: {len(urls)} categories, will process sequentially",
                    "info",
                )
                for idx, one_url in enumerate(urls, start=1):
                    if self.stop_event.is_set():
                        broadcaster.push(
                            f"[Multi-URL] Stop requested — aborting after "
                            f"{idx - 1}/{len(urls)} categories",
                            "warn",
                        )
                        break
                    broadcaster.push(
                        f"=== [Multi-URL {idx}/{len(urls)}] {one_url} ===",
                        "info",
                    )
                    per_params = {**params, "url": one_url}
                    try:
                        if use_multi:
                            await self._run_multi(action, per_params)
                        else:
                            await self._run_single(action, per_params)
                    except Exception as inner_e:
                        # One URL blowing up shouldn't kill the whole batch —
                        # log it and move on to the next so a single bad
                        # category doesn't waste the user's queued work.
                        broadcaster.push(
                            f"!!! [Multi-URL {idx}/{len(urls)}] {one_url}: "
                            f"{type(inner_e).__name__}: {inner_e}",
                            "error",
                        )
                        import traceback
                        broadcaster.push(traceback.format_exc(), "error")
                else:
                    broadcaster.push(
                        f"[Multi-URL] all {len(urls)} categories processed",
                        "info",
                    )
            else:
                if use_multi:
                    await self._run_multi(action, params)
                else:
                    await self._run_single(action, params)

            broadcaster.push(f"=== [{action}] finished ===", "info")
        except asyncio.CancelledError:
            broadcaster.push(f"=== [{action}] cancelled ===", "warn")
            raise
        except Exception as e:
            broadcaster.push(f"!!! [{action}] error: {e}", "error")
            import traceback
            broadcaster.push(traceback.format_exc(), "error")
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            self.scraper = None
            self.orchestrator = None

    async def _run_single(self, action: str, params: dict):
        """Legacy single-worker path (Phase 1B). Used when WORKER_COUNT==1 or
        for actions that don't benefit from parallelism (discover/setzip/list).

        Phase 5.5: single-worker actions (especially `list`) auto-pick whichever
        proxy slot is currently ALIVE — rather than always trying PROXIES[0].
        Per user 2026-05-17: "tool phải chọn profile ID nào với proxy đang còn
        live để bắt đầu làm".
        """
        if action in ("list", "discover", "setzip"):
            broadcaster.push(
                f"[Runner] '{action}' uses 1 worker by design "
                f"(WORKER_COUNT={cfg.WORKER_COUNT} only applies to detail scrape)",
                "info",
            )
        profile_id = ADSPOWER_PROFILE_ID

        # Phase 5.6: `list` action uses a DEDICATED profile + proxy from
        # config (LISTING_PROFILE_ID / LISTING_PROXY) when configured, so the
        # detail-scrape worker pool stays untouched. If LISTING_PROXY is empty,
        # fall through to the regular alive-slot picker below.
        listing_proxy = (getattr(cfg, "LISTING_PROXY", "") or "").strip()
        if action == "list" and listing_proxy:
            try:
                info = await asyncio.to_thread(
                    _bootstrap_listing_profile, listing_proxy,
                )
                profile_id = info["profile_id"]
                broadcaster.push(
                    f"[Profile] LISTING dedicated profile ready: id={profile_id} "
                    f"created={info['created']} proxy_updated={info['proxy_updated']} "
                    f"egress_ip={info['ip']} using_local={info['using_local']}",
                    "info",
                )
            except Exception as boot_err:
                broadcaster.push(
                    f"!!! LISTING profile bootstrap failed: {boot_err}",
                    "error",
                )
                broadcaster.push(
                    "Aborting run. Check LISTING_PROFILE_ID / LISTING_PROXY in config.py.",
                    "error",
                )
                return
            # Skip the worker-pool slot picker — we already have what we need.
            scraper = LowesScraper(profile_id=profile_id, worker_id="list", state=state)
            self.scraper = scraper
            scraper._ui_stop = self.stop_event
            try:
                await scraper.connect()
                await scraper.scrape_listing(
                    params["url"], params.get("page_from", 1), params.get("page_to")
                )
            finally:
                await scraper.close()
            return

        try:
            slot_idx, slot_proxy = await asyncio.to_thread(
                _pick_alive_slot, cfg.PROXIES
            )
            if slot_idx is None:
                # All proxies dead — fall back to local network for this run.
                broadcaster.push(
                    "[Profile] No proxy slot is currently alive — booting on "
                    "local network. Proxy will be retried at recovery time.",
                    "warn",
                )
                from profile_setup import _build_local_only_profile
                info = await asyncio.to_thread(
                    _build_local_only_profile,
                    ADSPOWER_API,
                    0,
                    cfg.PROFILES[0] if cfg.PROFILES else "",
                    "Scraper",
                    "all proxies dead at pre-flight",
                )
                profile_id = info["profile_id"]
                broadcaster.push(
                    f"[Profile] worker ready on local network: profile={profile_id}",
                    "info",
                )
            else:
                broadcaster.push(
                    f"[Profile] Picked alive slot[{slot_idx}] — "
                    f"bootstrapping (verify proxy + profile)...",
                    "info",
                )
                slot_profile = (
                    cfg.PROFILES[slot_idx]
                    if cfg.PROFILES and slot_idx < len(cfg.PROFILES)
                    else ""
                )
                info = await asyncio.to_thread(
                    setup_worker_profile,
                    ADSPOWER_API,
                    slot_idx,
                    slot_profile,
                    slot_proxy,
                    verify_proxy=False,         # already verified by _pick_alive_slot
                    fallback_to_local=True,     # safety net if slot flipped between probe and setup
                )
                profile_id = info["profile_id"]
                broadcaster.push(
                    f"[Profile] worker ready on slot[{slot_idx}]: "
                    f"profile={profile_id} created={info['created']} "
                    f"proxy_updated={info['proxy_updated']} egress_ip={info['ip']}",
                    "info",
                )
        except Exception as boot_err:
            broadcaster.push(f"!!! Profile bootstrap failed: {boot_err}", "error")
            broadcaster.push("Aborting run. Fix proxy/profile in config.py then retry.", "error")
            return

        scraper = LowesScraper(profile_id=profile_id, worker_id="ui", state=state)
        self.scraper = scraper
        scraper._ui_stop = self.stop_event

        try:
            await scraper.connect()
            if action == "discover":
                await scraper.discover()
            elif action == "setzip":
                await scraper.set_zip()
            elif action == "list":
                await scraper.scrape_listing(
                    params["url"], params.get("page_from", 1), params.get("page_to")
                )
            elif action == "detail":
                # URL is optional for detail (2026-05-20): when omitted, the
                # queue is consumed across ALL categories. `category_name`
                # (also optional) lets the UI dropdown pick from queued
                # categories without supplying a URL.
                await scraper.scrape_details_for_category(
                    params.get("url"),
                    category_name=params.get("category_name"),
                )
            elif action == "full":
                await scraper.set_zip()
                await scraper.scrape_listing(
                    params["url"], params.get("page_from", 1), params.get("page_to")
                )
                await scraper.scrape_details_for_category(params["url"])
            else:
                broadcaster.push(f"Unknown action: {action}", "error")
        finally:
            with contextlib.suppress(Exception):
                await scraper.close()

    async def _run_multi(self, action: str, params: dict):
        """Phase 4 multi-worker path. Worker 0 handles zip+listing in `full`,
        then all N workers consume the detail queue in parallel.

        URL is optional for `detail` (2026-05-20): when omitted, workers
        consume across ALL categories (no queue-filter). `full` still
        requires a URL since it has to run the listing scrape first.
        """
        url = params.get("url")
        if action == "full" and not url:
            broadcaster.push("`full` action requires a category URL", "error")
            return
        # `category_name` may come from the UI dropdown (no URL) or be
        # derived from `url` when one was supplied.
        category_name = params.get("category_name") or (_name_from_url(url) if url else None)

        # --- step 1: optional zip + listing (one-shot, worker 0 only) ---
        if action == "full":
            broadcaster.push("[Multi] Step 1/2 — worker 0 prep (zip + listing)", "info")
            try:
                # Reuse the single-worker path purely for the prep phase.
                # We construct a temporary LowesScraper bound to profile 0.
                info = await asyncio.to_thread(
                    setup_worker_profile,
                    ADSPOWER_API,
                    0,
                    cfg.PROFILES[0] if cfg.PROFILES else "",
                    cfg.PROXIES[0],
                    True,
                )
                prep = LowesScraper(
                    profile_id=info["profile_id"], worker_id="prep", state=state
                )
                prep._ui_stop = self.stop_event
                self.scraper = prep
                try:
                    await prep.connect()
                    await prep.set_zip()
                    await prep.scrape_listing(
                        url, params.get("page_from", 1), params.get("page_to")
                    )
                finally:
                    with contextlib.suppress(Exception):
                        await prep.close()
                    self.scraper = None
            except Exception as e:
                broadcaster.push(f"!!! prep phase failed: {e}", "error")
                return
            if self.stop_event.is_set():
                broadcaster.push("[Multi] Stop requested during prep — skipping detail phase", "warn")
                return

        # --- step 2: spawn workers on the detail queue ---
        # Decide effective worker count:
        #   1. User's per-run override (params["workers"]) wins, else cfg.WORKER_COUNT
        #   2. Cap to actual claimable rows — no point spawning 3 browsers for 1 job
        #   3. Floor at 1 (caller already checked there's work to do is implicit
        #      — if 0 claimable we still spawn 1 worker which will exit immediately)
        requested = params.get("workers") or cfg.WORKER_COUNT
        claimable = state.count_claimable(category_name)
        scope_label = f"'{category_name}'" if category_name else "the queue (all categories)"
        if claimable == 0:
            broadcaster.push(
                f"[Multi] No claimable jobs in {scope_label}. "
                f"Did you run 'list' first, or do you need 'Retry Failed' on the Dashboard?",
                "warn",
            )
            return
        effective = max(1, min(requested, claimable, cfg.WORKER_COUNT))
        if effective != requested:
            broadcaster.push(
                f"[Multi] Auto-cap: requested={requested}, claimable={claimable}, "
                f"pool={cfg.WORKER_COUNT} -> using {effective} worker(s)",
                "info",
            )

        label = "Step 2/2" if action == "full" else "Step 1/1"
        broadcaster.push(
            f"[Multi] {label} — spawning {effective} worker(s) on detail queue",
            "info",
        )
        orch = Orchestrator(
            state=state,
            worker_count=effective,
            profiles=cfg.PROFILES,
            proxies=cfg.PROXIES,
            stop_event=self.stop_event,
        )
        self.orchestrator = orch
        try:
            await orch.bootstrap(verify_proxy=True)
        except Exception as e:
            broadcaster.push(f"!!! Orchestrator bootstrap failed: {e}", "error")
            broadcaster.push(
                "Check PROFILES/PROXIES in config.py — every worker needs its own proxy.",
                "error",
            )
            return

        await orch.run_details(category_name, category_url=url)


runner = Runner()


# ─── API: state / queue ──────────────────────────────────────────────────

@app.get("/api/stats")
def api_stats(category: Optional[str] = None):
    return state.stats(category)


@app.get("/api/categories")
def api_categories():
    return {"categories": state.categories()}


@app.get("/api/jobs")
def api_jobs(
    status: Optional[str] = None,
    category: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
):
    """List jobs with optional filters."""
    limit = max(1, min(limit, 500))
    where = []
    params: list = []
    if status:
        where.append("status = ?")
        params.append(status)
    if category:
        where.append("category = ?")
        params.append(category)
    if q:
        where.append("(product_id LIKE ? OR url LIKE ?)")
        params.extend([f"%{q}%", f"%{q}%"])
    sql = "SELECT * FROM jobs"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY updated_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    with state._cursor() as cur:  # noqa: SLF001 - intentional
        rows = [dict(r) for r in cur.execute(sql, params).fetchall()]

        count_sql = "SELECT COUNT(*) FROM jobs"
        if where:
            count_sql += " WHERE " + " AND ".join(where)
        total = cur.execute(count_sql, params[:-2] if where else []).fetchone()[0]
    return {"jobs": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/api/job/{product_id}")
def api_job(product_id: str):
    job = state.get_job(product_id)
    if not job:
        raise HTTPException(404, "Job not found")
    # Phase 5.5: file may live under {pid}.json or {pid}_missimage.json
    from scraper import load_detail
    detail = load_detail(product_id)
    return {"job": job, "detail": detail}


# ─── API: proxy / profile (Phase 3) ──────────────────────────────────────

@app.get("/api/config/workers")
def api_config_workers(reload: int = 1):
    """Return the configured profile/proxy pool (passwords redacted).

    Always re-reads `config.py` from disk first (unless `?reload=0`) so that
    edits to PROFILES / PROXIES / WORKER_COUNT are picked up without restart.
    """
    if reload:
        reload_config()
    rows = []
    n = max(len(cfg.PROFILES), len(cfg.PROXIES), cfg.WORKER_COUNT)
    for i in range(n):
        prof = cfg.PROFILES[i] if i < len(cfg.PROFILES) else ""
        prx = cfg.PROXIES[i] if i < len(cfg.PROXIES) else ""
        redacted = prx
        if prx and ":" in prx and "@" in prx:
            # mask anything that looks like a password
            redacted = prx.replace(prx.split(":")[-1], "***")
        rows.append({
            "worker": i,
            "active": i < cfg.WORKER_COUNT,
            "profile": prof,
            "proxy": prx,
            "proxy_masked": redacted,
        })
    return {"workers": rows, "worker_count": cfg.WORKER_COUNT}


@app.post("/api/config/reload")
def api_config_reload():
    """Force-reload config.py from disk. Returns the freshly loaded tunables."""
    return {"ok": True, **reload_config()}


@app.post("/api/proxy/test")
def api_proxy_test(payload: dict):
    raw = (payload.get("proxy") or "").strip()
    if not raw:
        raise HTTPException(400, "proxy required")
    result = test_proxy_string(raw)
    return result


@app.post("/api/profile/setup")
def api_profile_setup(payload: dict):
    """Bootstrap one or all workers (find/create profile, bind proxy).

    Reloads config.py first so user edits are honored without server restart.
    """
    reload_config()
    worker = payload.get("worker")
    verify = bool(payload.get("verify", True))
    try:
        if worker is None:
            results = setup_all_workers(
                ADSPOWER_API, cfg.PROFILES, cfg.PROXIES, cfg.WORKER_COUNT,
                verify_proxy=verify,
            )
            return {"ok": True, "workers": results}
        else:
            i = int(worker)
            r = setup_worker_profile(
                ADSPOWER_API,
                worker_index=i,
                profile_id=cfg.PROFILES[i] if i < len(cfg.PROFILES) else None,
                proxy_raw=cfg.PROXIES[i] if i < len(cfg.PROXIES) else "",
                verify_proxy=verify,
            )
            return {"ok": True, "worker": r}
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))


@app.post("/api/state/release-stale")
def api_release_stale():
    n = state.release_stale()
    return {"released": n}


@app.post("/api/state/reconcile")
def api_reconcile(payload: dict):
    """Re-scan existing detail JSONs for a category and re-classify their DB status.

    Use after deleting state.db or after copying details in from elsewhere.
    """
    category = (payload.get("category") or "").strip()
    if not category:
        raise HTTPException(400, "category required")
    scraper = LowesScraper(worker_id="reconcile", state=state)
    summary = scraper.reconcile_existing_files(category)
    return summary


@app.post("/api/state/cleanup-stuck")
def api_cleanup_stuck():
    """Promote stuck `needs_refill` rows (attempts >= cap) to `failed`.

    Same logic that runs at app start, exposed so user can trigger it
    mid-runtime after lowering MAX_REFILL_ATTEMPTS in config.py.
    """
    n = state.cleanup_stuck_refills()
    return {"promoted": n}


@app.post("/api/state/retry-failed")
def api_retry_failed(payload: dict):
    """Re-arm all `failed` jobs in a category: move them back to `pending` and
    reset `attempts=0`. User clicks this after fixing whatever caused the
    failure (proxy / warm-up / selectors).

    If `category` is omitted, retries failed jobs across ALL categories.
    """
    category = (payload.get("category") or "").strip()
    import time as _t
    now = _t.time()
    with state._txn() as conn:  # noqa: SLF001
        if category:
            cur = conn.execute(
                "UPDATE jobs SET status='pending', attempts=0, last_error=NULL, "
                "missing_fields=NULL, updated_at=? "
                "WHERE category=? AND status='failed'",
                (now, category),
            )
        else:
            cur = conn.execute(
                "UPDATE jobs SET status='pending', attempts=0, last_error=NULL, "
                "missing_fields=NULL, updated_at=? "
                "WHERE status='failed'",
                (now,),
            )
        n = cur.rowcount
    return {"retried": n, "category": category or "ALL"}


@app.post("/api/state/requeue-refill")
def api_requeue_refill(payload: dict):
    """Move all 'done' jobs in a category back to 'needs_refill' so the next run
    re-checks them. Useful when REQUIRED_DETAIL_FIELDS rules tighten.
    """
    category = (payload.get("category") or "").strip()
    if not category:
        raise HTTPException(400, "category required")
    import time as _t
    now = _t.time()
    with state._txn() as conn:  # noqa: SLF001
        cur = conn.execute(
            "UPDATE jobs SET status='needs_refill', updated_at=? "
            "WHERE category=? AND status='done'",
            (now, category),
        )
        n = cur.rowcount
    return {"requeued": n}


# ─── API: runner ─────────────────────────────────────────────────────────

@app.get("/api/runner/status")
def api_runner_status():
    return runner.status()


@app.get("/api/workers/live")
def api_workers_live():
    """Snapshot of multi-worker orchestrator (Phase 4).

    Returns {"orchestrator": null} when running in single-worker mode or idle.
    """
    if runner.orchestrator is None:
        return {"orchestrator": None}
    return {"orchestrator": runner.orchestrator.snapshot()}


@app.post("/api/workers/{worker_id}/mark-solved")
def api_worker_mark_solved(worker_id: str):
    """Phase 5.1: signal a CF-blocked worker that the user has manually
    solved the challenge in AdsPower. Worker resumes its current product.
    """
    if runner.orchestrator is None:
        raise HTTPException(409, "No orchestrator running")
    for w in runner.orchestrator.workers:
        if w.worker_id == worker_id:
            ok = w.mark_cf_solved()
            if not ok:
                raise HTTPException(
                    409,
                    f"Worker {worker_id} is not in blocked_cf state (status={w.status}). "
                    f"Nothing to solve.",
                )
            return {"ok": True, "worker_id": worker_id}
    raise HTTPException(404, f"Worker {worker_id} not found")


@app.post("/api/runner/start")
async def api_runner_start(payload: dict):
    action = payload.get("action")
    if action not in ("discover", "setzip", "list", "detail", "full"):
        raise HTTPException(400, "Invalid action")
    params: dict[str, Any] = {}
    if action in ("list", "detail", "full"):
        # Accept either single `url` (legacy / CLI compat) or `urls` list
        # (Runner tab batch-mode, 2026-05-18). Both normalise to a list
        # internally so the Runner loops over it sequentially; downstream
        # code that expects `url` is still served by setting params["url"]
        # = urls[0] in the single-URL fast-path.
        #
        # For `detail` (2026-05-20): URL is OPTIONAL — the queue already
        # knows which products to scrape, so the field can be left empty
        # to consume jobs across ALL categories. list/full still require
        # at least one URL because they need to know where to start.
        urls_raw = payload.get("urls")
        urls: list[str] = []
        if urls_raw is not None:
            if not isinstance(urls_raw, list):
                raise HTTPException(400, "`urls` must be a list of strings")
            urls = [str(u).strip() for u in urls_raw if str(u).strip()]
        else:
            url = (payload.get("url") or "").strip()
            if url:
                urls = [url]
        if action in ("list", "full") and not urls:
            raise HTTPException(400, "url required for list/full")
        if urls:
            bad = next(
                (u for u in urls if not u.lower().startswith(("http://", "https://"))),
                None,
            )
            if bad:
                raise HTTPException(400, f"URL must start with http(s)://: {bad!r}")
            params["urls"] = urls
            if len(urls) == 1:
                params["url"] = urls[0]

        # category_names (2026-05-20): UI dropdown lets the user pick from
        # categories that already have queued jobs. detail-only — list/full
        # need actual URLs because they have to navigate to the page.
        cat_names_raw = payload.get("category_names")
        if cat_names_raw is not None:
            if action != "detail":
                raise HTTPException(
                    400,
                    "category_names only valid for action='detail' "
                    "(list/full need URLs, not just names)",
                )
            if not isinstance(cat_names_raw, list):
                raise HTTPException(400, "`category_names` must be a list of strings")
            cat_names = [str(n).strip() for n in cat_names_raw if str(n).strip()]
            if cat_names:
                params["category_names"] = cat_names
    if action in ("list", "full"):
        # Accept page_from / page_to either as ints or via 'pages' string '1-3'
        if "pages" in payload and payload["pages"]:
            pf, pt = _parse_pages(["--pages", str(payload["pages"])])
            params["page_from"], params["page_to"] = pf, pt
        else:
            params["page_from"] = int(payload.get("page_from") or 1)
            pt = payload.get("page_to")
            params["page_to"] = int(pt) if pt else None

    # Optional `workers` override (only meaningful for detail / full).
    # User leaves blank -> use cfg.WORKER_COUNT default. Auto-cap to pool size
    # and to the actual number of claimable jobs happens later in _run_multi.
    if action in ("detail", "full") and payload.get("workers") not in (None, "", 0):
        try:
            w = int(payload["workers"])
        except (TypeError, ValueError):
            raise HTTPException(400, "workers must be an integer")
        if w < 1:
            raise HTTPException(400, "workers must be >= 1")
        reload_config()
        if w > cfg.WORKER_COUNT:
            raise HTTPException(
                400,
                f"requested workers={w} exceeds pool size WORKER_COUNT={cfg.WORKER_COUNT}. "
                f"Edit config.py to enlarge the pool.",
            )
        params["workers"] = w

    await runner.start(action, params)
    return {"ok": True, "action": action, "params": params}


@app.post("/api/runner/stop")
async def api_runner_stop():
    await runner.stop()
    return {"ok": True}


# ─── WebSocket: live log ─────────────────────────────────────────────────

@app.websocket("/ws/logs")
async def ws_logs(ws: WebSocket):
    await ws.accept()
    q = broadcaster.subscribe()
    try:
        while True:
            entry = await q.get()
            await ws.send_json(entry)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        broadcaster.unsubscribe(q)


# ─── Static UI ───────────────────────────────────────────────────────────

if os.path.isdir(UI_DIR):
    app.mount("/ui", StaticFiles(directory=UI_DIR), name="ui")

# Expose existing viewer.html (in project root)
@app.get("/viewer.html")
def viewer_html():
    p = os.path.join(os.path.dirname(__file__), "viewer.html")
    if not os.path.exists(p):
        raise HTTPException(404, "viewer.html not found")
    return FileResponse(p)


# Expose details JSON for the existing viewer to load
@app.get("/data/details/{pid}.json")
def serve_detail(pid: str):
    # Reject anything that's not a clean ID
    if not pid.replace("-", "").replace("_", "").isalnum():
        raise HTTPException(400, "Invalid id")
    # Phase 5.5: support the _missimage variant filename. Strip the suffix
    # if it's part of the URL so /data/details/<pid>_missimage.json works,
    # and fall back to the standard name otherwise.
    from scraper import detail_path_existing
    real_pid = pid.removesuffix("_missimage")
    p = detail_path_existing(real_pid)
    if not p:
        raise HTTPException(404, "Not found")
    return FileResponse(p, media_type="application/json")


@app.get("/data/listings/{name}.json")
def serve_listing(name: str):
    safe = _safe_name(name)
    p = os.path.join(LISTINGS_DIR, f"{safe}.json")
    if not os.path.exists(p):
        raise HTTPException(404, "Not found")
    return FileResponse(p, media_type="application/json")


@app.get("/")
def root():
    index = os.path.join(UI_DIR, "index.html")
    if not os.path.exists(index):
        return JSONResponse({"error": "ui/index.html not found"}, status_code=500)
    return FileResponse(index)


# ─── Entry point ─────────────────────────────────────────────────────────

def _open_browser_when_ready(url: str, host: str, port: int):
    """Wait for the server to accept connections, then open the browser."""
    import socket
    for _ in range(50):
        try:
            with socket.create_connection((host, port), timeout=0.2):
                webbrowser.open(url)
                return
        except OSError:
            time.sleep(0.1)


def main():
    import uvicorn
    host = "127.0.0.1"
    port = 8765
    url = f"http://{host}:{port}/"
    print(f"[App] Starting Lowes Scraper UI at {url}")
    threading.Thread(
        target=_open_browser_when_ready, args=(url, host, port), daemon=True
    ).start()
    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
