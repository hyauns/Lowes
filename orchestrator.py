"""
Phase 4 — multi-worker orchestrator.

Responsibilities
----------------
1. Bootstrap N profiles up-front (one shot, sequential — AdsPower rate-limits).
2. Spin up N Worker tasks via asyncio.gather, each on its own AdsPower session.
3. Provide a single `stop_event` that propagates to every worker.
4. Recover claims from crashed runs (release_stale at startup).
5. Expose a live snapshot of all workers for the UI.

The orchestrator does NOT touch listings — caller is expected to have already
enqueued the jobs (`state.enqueue_listing(...)`) before calling `run_details`.
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

from config import ADSPOWER_API, PROFILES, PROXIES
from profile_setup import setup_all_workers
from state_manager import StateManager
from worker import Worker


class Orchestrator:
    """Coordinates N workers consuming the detail queue for one category."""

    def __init__(
        self,
        state: StateManager,
        worker_count: int,
        profiles: Optional[list[str]] = None,
        proxies: Optional[list[str]] = None,
        stop_event: Optional[asyncio.Event] = None,
    ):
        if worker_count < 1:
            raise ValueError("worker_count must be >= 1")
        self.state = state
        self.worker_count = worker_count
        self.profiles = profiles if profiles is not None else PROFILES
        self.proxies = proxies if proxies is not None else PROXIES
        self.stop_event = stop_event or asyncio.Event()

        self.workers: list[Worker] = []
        self._tasks: list[asyncio.Task] = []
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None
        self.bootstrap_results: list[dict] = []

    # ── bootstrap ────────────────────────────────────────────────────────

    async def bootstrap(self, verify_proxy: bool = True) -> list[dict]:
        """Set up all profiles + verify proxies. Blocks the loop briefly via to_thread."""
        print(f"[Orchestrator] Bootstrapping {self.worker_count} profile(s)...")
        results = await asyncio.to_thread(
            setup_all_workers,
            ADSPOWER_API,
            self.profiles,
            self.proxies,
            self.worker_count,
            verify_proxy,
        )
        self.bootstrap_results = results
        for r in results:
            local_tag = " [LOCAL-NET]" if r.get("using_local") else ""
            print(
                f"  worker{r['worker']}: profile={r['profile_id']} "
                f"egress_ip={r.get('ip')} created={r['created']} "
                f"proxy_updated={r['proxy_updated']}{local_tag}"
            )
        return results

    # ── run ──────────────────────────────────────────────────────────────

    async def run_details(self, category_name: str, category_url: str | None = None):
        """Spawn N workers and let them consume the detail queue.

        Caller must have already enqueued items via state.enqueue_listing.

        If `category_url` is provided, each worker runs `scraper.warmup(url)`
        right after connecting (Phase 5a): builds session cookies + Referer so
        /pd/ visits no longer look like cold direct hits.
        """
        if not self.bootstrap_results:
            raise RuntimeError("call bootstrap() before run_details()")

        # 1) Recover stale claims from previous crashed runs.
        released = self.state.release_stale()
        if released:
            print(f"[Orchestrator] Released {released} stale claim(s) from prior run")

        # 2) Build Worker objects (one per bootstrap result). Pass the FULL
        #    proxy list + this worker's slot index — Phase 5.2 needs them so
        #    a burned profile can be recreated with a sibling worker's proxy
        #    string (all 3 are rotating-IP, so re-using a slot still yields
        #    a fresh egress IP within ~30 min).
        self.workers = []
        for r in self.bootstrap_results:
            slot = int(r["worker"])
            # Phase 5.4: skip workers that failed bootstrap (e.g. AdsPower
            # rejected payload). Other workers must keep running — per user
            # "1-2 worker dead thì worker còn lại vẫn phải làm việc".
            if r.get("bootstrap_error") or not r.get("profile_id"):
                print(
                    f"[Orchestrator] skipping w{slot} — bootstrap failed: "
                    f"{r.get('bootstrap_error') or 'no profile_id'}"
                )
                continue
            w = Worker(
                worker_id=f"w{r['worker']}",
                profile_id=r["profile_id"],
                state=self.state,
                egress_ip=r.get("ip"),
                stop_event=self.stop_event,
                all_proxies=self.proxies,
                proxy_index=slot,
                using_local=bool(r.get("using_local")),
            )
            self.workers.append(w)

        if not self.workers:
            raise RuntimeError(
                "All workers failed to bootstrap. Check AdsPower + proxies + config."
            )

        self.started_at = time.time()

        # 3) Connect workers SEQUENTIALLY with a stagger.
        #    AdsPower's local API rate-limits to ~1 req/sec. Firing N
        #    `browser/start` calls via asyncio.gather makes the 2nd/3rd worker
        #    bounce with "Too many request per second" — confirmed in user logs.
        print(f"[Orchestrator] Connecting {len(self.workers)} workers to AdsPower...")
        ready: list[Worker] = []
        for i, w in enumerate(self.workers):
            if i > 0:
                # AdsPower needs ≥1s between profile starts; 1.5s is a safe margin.
                await asyncio.sleep(1.5)
            try:
                await w.connect()
                ready.append(w)
            except Exception as e:
                print(f"  [{w.worker_id}] connect FAILED: {e}")
                w.status = "error"
                w.last_error = str(e)[:200]

        if not ready:
            raise RuntimeError("All workers failed to connect. Check AdsPower + proxies.")

        if len(ready) < len(self.workers):
            print(
                f"[Orchestrator] {len(ready)}/{len(self.workers)} workers ready, "
                "continuing with the ones that connected."
            )

        # 4) Warm up each worker in parallel (Phase 5a). Each has its own browser
        #    + AdsPower session, so warming them concurrently is safe (no
        #    AdsPower API hits during this phase — pure Playwright on already-
        #    started browsers).
        if category_url:
            print(f"[Orchestrator] Warming up {len(ready)} worker(s) with category referer...")
            warmup_results = await asyncio.gather(
                *(w.scraper.warmup(category_url) for w in ready if w.scraper),
                return_exceptions=True,
            )
            for w, ok in zip(ready, warmup_results):
                if isinstance(ok, Exception):
                    print(f"  [{w.worker_id}] warmup raised: {ok} (worker still active)")
                elif not ok:
                    print(f"  [{w.worker_id}] warmup returned False (worker still active)")

        # 5) Spawn the queue-consumer task for each ready worker.
        try:
            self._tasks = [
                asyncio.create_task(w.run_queue(category_name), name=w.worker_id)
                for w in ready
            ]
            results = await asyncio.gather(*self._tasks, return_exceptions=True)
            for w, res in zip(ready, results):
                if isinstance(res, Exception) and not isinstance(res, asyncio.CancelledError):
                    print(f"  [{w.worker_id}] queue task error: {res}")
                    w.last_error = str(res)[:200]
                    w.status = "error"
        finally:
            # 5) Always tear down browsers, even on cancel.
            await asyncio.gather(
                *(w.close() for w in self.workers),
                return_exceptions=True,
            )
            self.finished_at = time.time()
            self._print_summary()

    def _print_summary(self):
        total_scraped = sum(w.scraped for w in self.workers)
        total_refilled = sum(w.refilled for w in self.workers)
        total_partial = sum(w.partial for w in self.workers)
        total_skipped = sum(w.skipped for w in self.workers)
        total_errors = sum(w.errors for w in self.workers)
        rec_google = sum(getattr(w, "recovered_via_google", 0) for w in self.workers)
        rec_swap = sum(getattr(w, "recovered_via_swap", 0) for w in self.workers)
        released = sum(getattr(w, "released_unrecovered", 0) for w in self.workers)
        cf_blocked = sum(getattr(w, "cf_blocked", 0) for w in self.workers)
        elapsed = (self.finished_at or time.time()) - (self.started_at or time.time())
        print(
            f"\n[Orchestrator] All workers done in {elapsed:.1f}s · "
            f"{total_scraped} scraped · {total_refilled} refilled · "
            f"{total_partial} partial · {total_skipped} skipped · {total_errors} errors"
        )
        if rec_google or rec_swap or released or cf_blocked:
            print(
                f"[Orchestrator] Recovery: {rec_google} via-Google · "
                f"{rec_swap} via-swap · {released} released · {cf_blocked} CF-blocked"
            )

    # ── stop / snapshot ──────────────────────────────────────────────────

    def request_stop(self):
        """Cooperative stop. Workers exit after their current item."""
        if not self.stop_event.is_set():
            print("[Orchestrator] Stop requested — workers will exit after current item")
            self.stop_event.set()

    def snapshot(self) -> dict:
        return {
            "running": self.started_at is not None and self.finished_at is None,
            "worker_count": self.worker_count,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "elapsed": (
                (self.finished_at or time.time()) - self.started_at
                if self.started_at else 0
            ),
            "stop_requested": self.stop_event.is_set(),
            "workers": [w.snapshot() for w in self.workers],
        }
