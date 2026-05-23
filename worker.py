"""
Phase 4 — single worker process inside the orchestrator.

A Worker owns:
  • one AdsPower profile (already bootstrapped by Orchestrator)
  • one LowesScraper instance (its own CDP connection + page)
  • a cooperative `stop_event` shared by the orchestrator

It loops on `state.claim_next(worker_id, category)` until the queue is empty
or the stop signal fires. Atomic claim in StateManager prevents two workers
from grabbing the same product_id.

Snapshot
--------
Each worker exposes `.snapshot()` so the UI can poll live status without
touching the scraper internals.
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Optional

from block_detector import detect_block
import config as _cfg
from completeness import check_completeness
from config import DETAILS_DIR, MAX_REFILL_ATTEMPTS

# Soft-import the Phase 5.7 tunables so existing config.py files without
# these keys keep working (avoids ImportError on VPS that hasn't been
# updated yet). Defaults match config.example.py.
ITEM_TIMEOUT_SECONDS = getattr(_cfg, "ITEM_TIMEOUT_SECONDS", 240)
MAX_CONSECUTIVE_CRASHES_PER_PID = getattr(_cfg, "MAX_CONSECUTIVE_CRASHES_PER_PID", 3)
from scraper import (
    BrowserClosedError,
    LowesScraper,
    PageCrashedError,
    ProxyDeadError,
    _is_browser_closed_error,
    _is_page_crashed_error,
    _load_json,
    load_detail,
)
from state_manager import StateManager


class WorkerError(RuntimeError):
    pass


class Worker:
    """One scraping worker — owns one LowesScraper / one AdsPower session."""

    def __init__(
        self,
        worker_id: str,
        profile_id: str,
        state: StateManager,
        egress_ip: Optional[str] = None,
        stop_event: Optional[asyncio.Event] = None,
        all_proxies: Optional[list[str]] = None,
        proxy_index: int = 0,
        using_local: bool = False,
    ):
        self.worker_id = worker_id
        self.profile_id = profile_id
        self.state = state
        self.egress_ip = egress_ip
        self.stop_event = stop_event or asyncio.Event()

        # Phase 5.2/5.3: proxy pool. `all_proxies` is the full PROXIES list
        # from config; `proxy_index` is the slot the worker is currently bound
        # to (-1 means "no proxy / local network"). `alt_proxies` is retained
        # for code that still reads it but is no longer used for routing —
        # swap_to_alive_proxy tests the whole pool live.
        self.all_proxies: list[str] = list(all_proxies or [])
        self.proxy_index: int = proxy_index
        self.alt_proxies: list[str] = [
            p for i, p in enumerate(self.all_proxies)
            if i != proxy_index and p and p.strip()
        ]
        # Phase 5.3: True after switch_to_local_network OR if the orchestrator
        # bootstrapped this worker onto local network (proxy was dead at boot).
        # If a block hits a local-network worker, recovery escalates to
        # swap_to_alive_proxy (delete + pick alive proxy or stay local).
        self.using_local: bool = using_local

        self.scraper: Optional[LowesScraper] = None
        self.current_pid: Optional[str] = None
        self.status: str = "init"   # init | connecting | idle | scraping | recovering | stopping | done | error
        self.started_at: Optional[float] = None
        self.item_started_at: Optional[float] = None
        self.last_error: Optional[str] = None

        # Counters (per-run)
        self.scraped = 0
        self.refilled = 0
        self.partial = 0
        self.skipped = 0
        self.errors = 0
        # Phase 5.2 recovery stats
        self.recovered_via_google = 0
        self.recovered_via_swap = 0
        self.released_unrecovered = 0
        self.cf_blocked = 0
        # Phase 5.3 — proxy-death recovery stats
        self.proxy_dead_recoveries = 0       # successful switch_to_local
        self.proxy_dead_failures = 0          # switch_to_local failed
        # Page-crash recovery (renderer died but browser still alive)
        self.page_crashes = 0
        self.page_crash_recoveries = 0
        # Track consecutive crashes on the SAME pid so a poison page (one
        # that crashes every worker) gets escalated to failed instead of
        # bouncing forever between release → claim → crash → release.
        self._last_crash_pid: Optional[str] = None
        self._same_pid_crash_count: int = 0
        # Per-item watchdog: how many times we hit ITEM_TIMEOUT_SECONDS.
        self.item_timeouts = 0

        # Phase 5.1 — Cloudflare manual-solve pause state.
        # When CF is detected, worker enters status='blocked_cf', sets
        # cf_pending_detail (visible to UI), and awaits cf_solve_event.
        # The Event is fired by POST /api/workers/{id}/mark-solved.
        self.cf_solve_event: asyncio.Event = asyncio.Event()
        self.cf_pending_detail: Optional[str] = None
        self.cf_pending_pid: Optional[str] = None

    # ── lifecycle ────────────────────────────────────────────────────────

    async def connect(self):
        """Start AdsPower browser + Playwright. Blocking parts use asyncio.to_thread."""
        self.status = "connecting"
        self.started_at = time.time()
        self.scraper = LowesScraper(
            profile_id=self.profile_id,
            worker_id=self.worker_id,
            state=self.state,
        )
        self.scraper._ui_stop = self.stop_event
        await self.scraper.connect()
        self.status = "idle"

    async def close(self):
        if self.scraper is not None:
            try:
                await self.scraper.close()
            except Exception as e:
                print(f"  [{self.worker_id}] close error (ignored): {e}")
        self.scraper = None
        if self.status not in ("error", "stopping"):
            self.status = "done"

    # ── queue loop ───────────────────────────────────────────────────────

    async def run_queue(self, category_name: str):
        """Consume jobs from StateManager until empty or stop_event."""
        if self.scraper is None:
            raise WorkerError(f"{self.worker_id}: connect() not called")

        print(f"\n[{self.worker_id}] consuming queue for '{category_name}'...")
        try:
            while True:
                if self.stop_event.is_set():
                    self.status = "stopping"
                    print(f"  [{self.worker_id}] stop requested — exiting loop")
                    break

                job = self.state.claim_next(self.worker_id, category=category_name)
                if not job:
                    print(f"  [{self.worker_id}] no more jobs")
                    break

                self.current_pid = job["product_id"]
                self.item_started_at = time.time()
                self.status = "scraping"

                # Per-item watchdog. Without this, any Playwright op that
                # hangs silently (renderer crashed but no error raised, JS
                # dialog blocking, network stall) leaves the worker stuck
                # forever on one product — the "browser open but worker idle"
                # symptom on VPS. asyncio.wait_for cancels the inner task and
                # raises TimeoutError, which we handle as a page-crash-class
                # recovery (release pid + replace page).
                try:
                    await asyncio.wait_for(
                        self._process_job(job),
                        timeout=ITEM_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    self.item_timeouts += 1
                    await self._handle_page_crash(
                        self.current_pid or job["product_id"],
                        f"item watchdog timed out after {ITEM_TIMEOUT_SECONDS}s",
                    )

                self.current_pid = None
                self.item_started_at = None

                # tiny breathe between items
                await asyncio.sleep(0)

        except asyncio.CancelledError:
            # Hard cancel — release whatever we were holding
            if self.current_pid:
                print(f"  [{self.worker_id}] cancelled — releasing {self.current_pid}")
                try:
                    self.state.release(self.current_pid)
                except Exception:
                    pass
            self.status = "stopping"
            raise
        except KeyboardInterrupt:
            if self.current_pid:
                print(f"  [{self.worker_id}] Ctrl+C — releasing {self.current_pid}")
                try:
                    self.state.release(self.current_pid)
                except Exception:
                    pass
            raise
        finally:
            print(
                f"[{self.worker_id}] queue exit: "
                f"{self.scraped} scraped · {self.refilled} refilled · "
                f"{self.partial} partial · {self.skipped} skipped · {self.errors} errors"
            )
            if self.status == "scraping":
                self.status = "idle"

    async def _process_job(self, job: dict):
        """Scrape one job. Mark done / needs_refill / failed accordingly."""
        pid = job["product_id"]
        purl = job["url"]
        was_refill = bool(job.get("missing_fields"))

        # Phase 5.5: existing file may be saved as either {pid}.json or
        # {pid}_missimage.json — use the dual-path helper.
        existing = load_detail(pid)
        if existing:
            is_complete, _ = check_completeness(existing)
            if is_complete:
                self.state.mark_done(pid)
                self.skipped += 1
                print(f"  [{self.worker_id}] complete (skip): {pid}")
                return

        try:
            label = "Refill" if (existing or was_refill) else "Scraping"
            print(f"  [{self.worker_id}] {label} {pid} ...")
            result = await self.scraper.scrape_detail(purl, pid)

            # Phase 5.x: classify the page state before judging completeness.
            # A `partial` page that's actually behind an Access Denied or
            # Cloudflare wall shouldn't count as a real partial — it's a block.
            block = await detect_block(self.scraper.page)

            # Phase 5.5: detect_block reads the LIVE page; by the time it
            # runs, the page may have moved on. As a defence-in-depth check,
            # also look for block indicators in the scraped RESULT dict (e.g.
            # title="Access Denied" scraped before AD interstitial cleared).
            if block["kind"] == "ok":
                result_block = result.pop("_blocked", None) if isinstance(result, dict) else None
                if result_block:
                    block = result_block

            if block["kind"] != "ok":
                handled = await self._handle_block(block, pid, purl)
                if handled == "retry":
                    # Recovery rescraped successfully. Re-extract.
                    result = await self.scraper.scrape_detail(purl, pid)
                    # Strip residual marker so completeness doesn't see it
                    if isinstance(result, dict):
                        result.pop("_blocked", None)
                elif handled == "released":
                    # Item returned to pending; this job is done for us.
                    return
                elif handled == "cf_paused":
                    # 5.1 will implement actual pause — for now log + release.
                    return

            is_complete, missing = check_completeness(result)
            if is_complete:
                self.state.mark_done(pid)
                if existing:
                    self.refilled += 1
                else:
                    self.scraped += 1
            else:
                # `job["attempts"]` is the value BEFORE claim_next's increment,
                # so the just-completed run is attempt number (job["attempts"] + 1).
                this_attempt = (job.get("attempts") or 0) + 1
                if this_attempt >= MAX_REFILL_ATTEMPTS:
                    err = (
                        f"partial after {this_attempt} attempts — "
                        f"missing {missing} (likely soft-blocked or fields absent)"
                    )
                    self.state.mark_failed(pid, err)
                    self.errors += 1
                    print(f"  [{self.worker_id}] FAILED {pid}: {err}")
                else:
                    self.state.mark_needs_refill(pid, missing)
                    self.partial += 1
                    print(
                        f"  [{self.worker_id}] partial {pid} "
                        f"(attempt {this_attempt}/{MAX_REFILL_ATTEMPTS}): missing {missing}"
                    )
        except asyncio.CancelledError:
            # Don't mark failed — orchestrator is shutting us down
            try:
                self.state.release(pid)
            except Exception:
                pass
            raise
        except PageCrashedError as e:
            # The Chrome renderer for our tab died. Cheap recovery — replace
            # the page in the same context (no AdsPower restart) and let the
            # released pid be reclaimed on the next loop tick.
            await self._handle_page_crash(pid, str(e))
        except BrowserClosedError as e:
            # Phase 5.3 hotfix: the underlying AdsPower browser is gone
            # (CDP dropped, profile stopped, recovery closed it). Reconnect
            # in place — don't grind through page operations that all fail
            # with the same "Target page closed" error.
            print(f"  [{self.worker_id}] BROWSER CLOSED on {pid}: {e}")
            self.last_error = f"browser_closed: {str(e)[:160]}"
            try:
                self.state.release(pid)
            except Exception:
                pass
            ok = await self._reconnect_after_browser_closed()
            if not ok:
                self.status = "error"
                raise
        except ProxyDeadError as e:
            # Phase 5.3: proxy died mid-scrape. Release the item (it'll be
            # re-claimed by whoever grabs it next, possibly us after recovery)
            # and switch to local network.
            print(f"  [{self.worker_id}] PROXY DEAD on {pid}: {e}")
            self.last_error = f"proxy_dead: {str(e)[:160]}"
            try:
                self.state.release(pid)
            except Exception:
                pass
            ok = await self._handle_proxy_dead()
            if not ok:
                # Couldn't recover — bail out so orchestrator can decide.
                self.status = "error"
                raise
        except Exception as e:
            # Late promotion: scrape_detail has many raw page.evaluate() /
            # locator calls that aren't wrapped in our typed errors. If one
            # of them raised 'Target crashed' / 'Page crashed' / browser-
            # closed, treat it like the typed version instead of mark_failed
            # (which would burn through the whole queue marking everything
            # failed and starve sibling workers — the "3 → 1 workers" bug).
            if _is_page_crashed_error(e):
                await self._handle_page_crash(pid, str(e))
                return
            if _is_browser_closed_error(e):
                print(f"  [{self.worker_id}] BROWSER CLOSED on {pid} (late): {e}")
                self.last_error = f"browser_closed: {str(e)[:160]}"
                try:
                    self.state.release(pid)
                except Exception:
                    pass
                ok = await self._reconnect_after_browser_closed()
                if not ok:
                    self.status = "error"
                    raise
                return
            print(f"  [{self.worker_id}] error {pid}: {e}")
            try:
                self.state.mark_failed(pid, str(e))
            except Exception:
                pass
            self.errors += 1
            self.last_error = str(e)[:200]

    # ── page-crash recovery (renderer died, browser still alive) ─────────

    async def _handle_page_crash(self, pid: str, err: str) -> None:
        """Replace the dead tab and decide whether to retry the pid or fail it.

        Normal path: release pid → recover_page → next claim_next retries it.

        Same-pid loop guard: if THIS pid has already crashed
        MAX_CONSECUTIVE_CRASHES_PER_PID times in a row on this worker,
        mark_failed instead of release so we don't bounce on a poison
        product (one that crashes every renderer). User can re-arm via
        UI "Retry Failed".

        If the cheap page-replace fails, escalate to a full browser reconnect.
        """
        print(f"  [{self.worker_id}] PAGE CRASHED on {pid}: {err}")
        self.last_error = f"page_crashed: {err[:160]}"
        self.page_crashes += 1

        # Track consecutive crashes on the same product
        if self._last_crash_pid == pid:
            self._same_pid_crash_count += 1
        else:
            self._last_crash_pid = pid
            self._same_pid_crash_count = 1

        if self._same_pid_crash_count >= MAX_CONSECUTIVE_CRASHES_PER_PID:
            try:
                self.state.mark_failed(
                    pid,
                    f"page crashed {self._same_pid_crash_count} times in a row: {err[:200]}",
                )
            except Exception:
                pass
            self.errors += 1
            self._same_pid_crash_count = 0
            self._last_crash_pid = None
            print(
                f"  [{self.worker_id}] [page-crash] {pid} crashed too many "
                f"times — marked failed"
            )
        else:
            try:
                self.state.release(pid)
            except Exception:
                pass

        self.status = "recovering"
        ok = False
        if self.scraper is not None:
            try:
                ok = await self.scraper.recover_page()
            except Exception as e:
                print(f"  [{self.worker_id}] [page-crash] recover_page raised: {e}")
                ok = False
        if not ok:
            print(
                f"  [{self.worker_id}] [page-crash] page-replace failed "
                f"— escalating to full browser reconnect"
            )
            ok = await self._reconnect_after_browser_closed()
            if not ok:
                self.status = "error"
                raise WorkerError(f"{self.worker_id}: page-crash recovery failed")
        self.page_crash_recoveries += 1
        self.status = "idle"

    # ── Cloudflare pause (Phase 5.1) ─────────────────────────────────────

    async def _pause_for_cf_solve(self, pid: str, detail: str) -> bool:
        """Hold the worker in 'blocked_cf' until UI signals solve.

        The user opens the worker's AdsPower window (visible from
        AdsPower UI, profile_id is in the worker snapshot), passes the
        Cloudflare challenge manually, then clicks
        `POST /api/workers/{worker_id}/mark-solved` (UI button) which
        sets cf_solve_event.

        Returns True if user signaled solve, False if stop_event fired
        (cooperative shutdown — caller releases the claim).
        """
        self.status = "blocked_cf"
        self.cf_pending_pid = pid
        self.cf_pending_detail = detail
        # Clear in case a previous solve signal leaked into the event.
        self.cf_solve_event.clear()

        print(
            f"  [{self.worker_id}] [CF] PAUSED on product {pid}. "
            f"Open AdsPower profile={self.profile_id} → solve challenge → "
            f"click 'Mark CF Solved' in the UI."
        )

        # Wait for either user-solve or worker stop. Whichever fires first wins.
        solve_task = asyncio.create_task(self.cf_solve_event.wait())
        stop_task = asyncio.create_task(self.stop_event.wait())
        try:
            done, pending = await asyncio.wait(
                {solve_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
        except asyncio.CancelledError:
            solve_task.cancel()
            stop_task.cancel()
            raise

        if self.stop_event.is_set():
            print(f"  [{self.worker_id}] [CF] stop fired while paused — abandoning solve")
            self.cf_pending_pid = None
            self.cf_pending_detail = None
            return False

        print(f"  [{self.worker_id}] [CF] resumed by user — re-checking page state")
        self.cf_pending_pid = None
        self.cf_pending_detail = None
        return True

    def mark_cf_solved(self) -> bool:
        """Signal that a paused worker can resume. Called by the API endpoint.

        Returns True if the worker was actually paused on CF (event fired),
        False if nothing to signal (caller can 404 / no-op).
        """
        if self.status != "blocked_cf":
            return False
        self.cf_solve_event.set()
        return True

    # ── block recovery (Phase 5.2) ───────────────────────────────────────

    async def _handle_block(self, block: dict, pid: str, purl: str) -> str:
        """Run the recovery ladder for a detected block.

        Returns one of:
          - "retry"     : page is clean now, caller should re-extract
          - "released"  : item put back to pending and skipped
          - "cf_paused" : Cloudflare detected — Phase 5.1 will pause here.
                          For now we release+continue with the next item.
        """
        from recovery import (
            google_detour,
            rotate_and_recreate,
            swap_to_alive_proxy,
            release_to_pending,
        )

        kind = block.get("kind")
        detail = block.get("detail", "")
        self.status = "recovering"
        print(f"  [{self.worker_id}] BLOCK detected on {pid}: kind={kind} | {detail}")

        # Cloudflare → pause and wait for the user to solve in AdsPower
        # window, then click "Mark CF Solved" in the UI. The claim is held
        # the entire time so no other worker steals this product.
        if kind == "cloudflare":
            self.cf_blocked += 1
            solved = await self._pause_for_cf_solve(pid, detail)
            if not solved:
                # Stop event fired during pause — release cleanly.
                release_to_pending(
                    self.state, pid,
                    f"cloudflare challenge — stop signaled while paused: {detail}",
                )
                self.released_unrecovered += 1
                return "released"

            # Re-evaluate page after user said they solved it.
            verdict = await detect_block(self.scraper.page)
            new_kind = verdict.get("kind")
            if new_kind == "ok":
                self.status = "scraping"
                return "retry"
            if new_kind == "cloudflare":
                # User marked solved but CF still showing → don't loop forever.
                # Surrender this item, let user re-arm from UI later.
                print(
                    f"  [{self.worker_id}] [CF] marked solved but still blocked "
                    f"— releasing {pid}"
                )
                release_to_pending(
                    self.state, pid,
                    f"cloudflare: marked solved but page still challenged",
                )
                self.released_unrecovered += 1
                return "released"
            # Anything else (e.g. now AD) → recurse into the AD ladder.
            return await self._handle_block(verdict, pid, purl)

        # Access Denied → escalation ladder
        if kind == "access_denied":
            # Tier 1: Google detour — skip if we're on local network (Google
            # would just SERP the same blocked egress IP).
            if not self.using_local:
                ok = await google_detour(self.scraper, pid, purl)
                if ok:
                    self.recovered_via_google += 1
                    self.status = "scraping"
                    return "retry"
            else:
                print(
                    f"  [{self.worker_id}] [recover] on local network — "
                    f"skipping Google detour, going straight to proxy swap"
                )

            # Tier 2 (Phase 5.4): rotate the current proxy's IP via
            # proxyxoay API and recreate the profile. Only meaningful if we
            # ARE on a proxy slot (skip if local — no rotation key for "local").
            if not self.using_local:
                ok = await rotate_and_recreate(self)
                if ok:
                    try:
                        await self.scraper.warmup(purl.rsplit("/pd/", 1)[0])
                    except Exception:
                        pass
                    try:
                        await self.scraper.scrape_detail(purl, pid)
                        verdict = await detect_block(self.scraper.page)
                        if verdict["kind"] == "ok":
                            self.recovered_via_swap += 1
                            self.status = "scraping"
                            return "retry"
                        print(
                            f"  [{self.worker_id}] [recover/rotate] post-rotate still blocked: "
                            f"{verdict['kind']}"
                        )
                    except Exception as e:
                        print(f"  [{self.worker_id}] [recover/rotate] post-rotate scrape error: {e}")

            # Tier 3: swap to a DIFFERENT alive proxy from the pool. Either we
            # were on local (no key to rotate), or rotation failed / didn't
            # actually move the IP.
            ok = await swap_to_alive_proxy(self)
            if ok:
                try:
                    await self.scraper.warmup(purl.rsplit("/pd/", 1)[0])
                except Exception:
                    pass
                try:
                    await self.scraper.scrape_detail(purl, pid)
                    verdict = await detect_block(self.scraper.page)
                    if verdict["kind"] == "ok":
                        self.recovered_via_swap += 1
                        self.status = "scraping"
                        return "retry"
                    print(
                        f"  [{self.worker_id}] [recover/swap-proxy] post-swap still blocked: "
                        f"{verdict['kind']}"
                    )
                except Exception as e:
                    print(f"  [{self.worker_id}] [recover/swap-proxy] post-swap scrape error: {e}")

            # Tier 4: give up on this item, release back to pending so user can
            # Retry from the UI. Worker continues with next claim.
            release_to_pending(
                self.state, pid,
                f"access_denied: ladder exhausted ({detail})",
            )
            self.released_unrecovered += 1
            self.status = "scraping"
            return "released"

        # Unknown kind — treat as transient, just continue normally.
        self.status = "scraping"
        return "retry"

    # ── reconnect after browser closed (Phase 5.3 hotfix) ────────────────

    async def _reconnect_after_browser_closed(self) -> bool:
        """The AdsPower browser died but the profile/proxy are still valid.

        Try a plain reconnect (close → connect with the SAME profile_id).
        No proxy change, no profile recreation. If that fails, fall through
        to proxy-dead handler (which may flip to local network).
        """
        self.status = "recovering"
        try:
            if self.scraper:
                await self.scraper.close()
        except Exception:
            pass
        try:
            await self.connect()
            if self.scraper is not None:
                self.scraper._warmed_up = False
                self.scraper._proxy_error_count = 0
            self.status = "idle"
            print(f"  [{self.worker_id}] [reconnect] back online")
            return True
        except Exception as e:
            from adspower_helper import is_profile_missing_msg

            # If the profile itself is GONE (deleted by a prior recovery, or a
            # stale config id), flipping it to local network is impossible —
            # there's no profile to flip. Go straight to a full rebuild:
            # delete (no-op) → find an alive proxy from config.PROXIES (or fall
            # back to local network if none answer) → create a fresh profile →
            # reconnect. This is exactly what the user asked for: never loop on
            # "profile not exists", always recreate.
            if is_profile_missing_msg(e):
                from recovery import swap_to_alive_proxy
                print(
                    f"  [{self.worker_id}] [reconnect] profile {self.profile_id} is "
                    f"GONE — rebuilding a fresh profile (alive proxy from config "
                    f"or local network)"
                )
                self.status = "recovering"
                ok = await swap_to_alive_proxy(self)
                if ok:
                    self.proxy_dead_recoveries += 1
                    self.status = "idle"
                    return True
                self.proxy_dead_failures += 1
                return False

            print(f"  [{self.worker_id}] [reconnect] failed: {e} — escalating to proxy-dead handler")
            return await self._handle_proxy_dead()

    # ── proxy-dead recovery (Phase 5.3) ──────────────────────────────────

    async def _handle_proxy_dead(self) -> bool:
        """Run the proxy-death ladder.

        Step 1: switch the current profile to local network (no delete). If
                that succeeds, the worker resumes and the item we released
                will be re-claimed on the next loop tick.
        Step 2: if local switch fails, fall back to full profile swap
                (delete + create new with alive proxy / or local).

        Returns True if worker is usable again, False if everything failed.
        """
        from recovery import switch_to_local_network, swap_to_alive_proxy

        self.status = "recovering"
        ok = await switch_to_local_network(self)
        if ok:
            self.proxy_dead_recoveries += 1
            self.status = "idle"
            return True

        print(
            f"  [{self.worker_id}] [recover/proxy-dead] local-network switch failed — "
            f"escalating to full profile swap"
        )
        ok = await swap_to_alive_proxy(self)
        if ok:
            self.proxy_dead_recoveries += 1
            self.status = "idle"
            return True

        self.proxy_dead_failures += 1
        return False

    # ── snapshot for UI / logging ────────────────────────────────────────

    def snapshot(self) -> dict:
        now = time.time()
        return {
            "worker_id": self.worker_id,
            "profile_id": self.profile_id,
            "proxy_index": self.proxy_index,
            "egress_ip": self.egress_ip,
            "status": self.status,
            "current_pid": self.current_pid,
            "started_at": self.started_at,
            "elapsed": (now - self.started_at) if self.started_at else 0,
            "item_elapsed": (now - self.item_started_at) if self.item_started_at else 0,
            "scraped": self.scraped,
            "refilled": self.refilled,
            "partial": self.partial,
            "skipped": self.skipped,
            "errors": self.errors,
            "recovered_via_google": self.recovered_via_google,
            "recovered_via_swap": self.recovered_via_swap,
            "released_unrecovered": self.released_unrecovered,
            "cf_blocked": self.cf_blocked,
            "cf_pending_pid": self.cf_pending_pid,
            "cf_pending_detail": self.cf_pending_detail,
            "using_local": self.using_local,
            "proxy_dead_recoveries": self.proxy_dead_recoveries,
            "proxy_dead_failures": self.proxy_dead_failures,
            "page_crashes": self.page_crashes,
            "page_crash_recoveries": self.page_crash_recoveries,
            "item_timeouts": self.item_timeouts,
            "last_error": self.last_error,
        }
