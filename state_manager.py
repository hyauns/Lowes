"""
SQLite-backed job queue for multi-worker scraping.

Design notes
------------
- WAL mode + NORMAL sync => safe concurrency + good throughput.
- busy_timeout + manual retry loop => robust against transient locks.
- One connection per *thread/coroutine* (sqlite3 connections are not thread-safe by default).
- Atomic claim via `BEGIN IMMEDIATE` + status check inside the txn.
- Stale-claim recovery: if a worker died, its claimed jobs become available again
  after CLAIM_STALE_SECONDS.

Job lifecycle
-------------
  pending  ─┐
            ├─► claimed ──► done
            │              └► failed (transient) ──► pending (auto-release)
            └─► needs_refill ──► claimed (refill run)
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Iterable, Optional

from config import (
    STATE_DB_PATH,
    STATE_DB_BACKUP,
    CLAIM_STALE_SECONDS,
    SQLITE_BUSY_RETRIES,
    MAX_REFILL_ATTEMPTS,
)


# ─── Schema ──────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    product_id    TEXT PRIMARY KEY,
    url           TEXT NOT NULL,
    category      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    worker_id     TEXT,
    claimed_at    REAL,
    completed_at  REAL,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    missing_fields TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status      ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_category    ON jobs(category);
CREATE INDEX IF NOT EXISTS idx_jobs_claimed_at  ON jobs(claimed_at);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

VALID_STATUSES = {"pending", "claimed", "done", "failed", "needs_refill"}


# ─── Connection helpers ──────────────────────────────────────────────────

_local = threading.local()


def _connect(db_path: str) -> sqlite3.Connection:
    """Open a tuned SQLite connection. One per thread."""
    conn = sqlite3.connect(
        db_path,
        timeout=30.0,           # client-side wait if file is locked
        isolation_level=None,   # autocommit; we manage txns explicitly
        check_same_thread=True,
    )
    conn.row_factory = sqlite3.Row
    # PRAGMAs — order matters
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def _retry_busy(fn, *args, **kwargs):
    """Run fn, retrying on sqlite3.OperationalError 'database is locked' / 'busy'."""
    last = None
    for attempt in range(SQLITE_BUSY_RETRIES):
        try:
            return fn(*args, **kwargs)
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                last = e
                time.sleep(0.1 * (2 ** attempt))
                continue
            raise
    raise last  # type: ignore[misc]


# ─── StateManager ────────────────────────────────────────────────────────

class StateManager:
    """Job queue manager. Each worker should construct its own instance.

    Usage
    -----
        sm = StateManager()
        sm.enqueue_listing("power-tools", [(pid, url), ...])
        while job := sm.claim_next(worker_id="w0"):
            try:
                ...
                sm.mark_done(job["product_id"])
            except Exception as e:
                sm.mark_failed(job["product_id"], str(e))
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or STATE_DB_PATH
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self._maybe_backup()
        self._init_schema()
        self._integrity_check()
        # One-shot cleanup: any `needs_refill` row with attempts already past
        # the configured cap is effectively dead. Promote to `failed` so:
        #   (a) it stops being re-claimed (priority would keep grabbing it)
        #   (b) the Dashboard's "Retry Failed" button counts it correctly
        # This rescues rows enqueued before MAX_REFILL_ATTEMPTS landed.
        n = self.cleanup_stuck_refills()
        if n:
            print(
                f"[State] Cleanup: {n} stuck needs_refill row(s) "
                f"(attempts >= {MAX_REFILL_ATTEMPTS}) -> failed"
            )

    # ── lifecycle ────────────────────────────────────────────────────

    def _maybe_backup(self):
        """Snapshot the DB file at startup. Cheap insurance against corruption."""
        if os.path.exists(self.db_path):
            try:
                shutil.copy2(self.db_path, STATE_DB_BACKUP)
            except OSError as e:
                print(f"[State] Backup failed (non-fatal): {e}")

    def _init_schema(self):
        with self._cursor() as cur:
            cur.executescript(_SCHEMA)

    def _integrity_check(self):
        with self._cursor() as cur:
            row = cur.execute("PRAGMA integrity_check").fetchone()
            if row and row[0] != "ok":
                raise RuntimeError(
                    f"[State] SQLite integrity_check failed: {row[0]}. "
                    f"Restore from {STATE_DB_BACKUP} or delete {self.db_path}."
                )

    def close(self):
        conn = getattr(_local, "conn", None)
        if conn is not None:
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.Error:
                pass
            conn.close()
            _local.conn = None

    # ── connection per thread ────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(_local, "conn", None)
        if conn is None:
            conn = _connect(self.db_path)
            _local.conn = conn
        return conn

    @contextmanager
    def _cursor(self):
        conn = self._conn()
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    @contextmanager
    def _txn(self):
        """Write transaction with IMMEDIATE lock + busy retry."""
        conn = self._conn()

        def _begin():
            conn.execute("BEGIN IMMEDIATE")

        _retry_busy(_begin)
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    # ── enqueue ──────────────────────────────────────────────────────

    def enqueue_listing(
        self,
        category: str,
        items: Iterable[tuple[str, str]],
    ) -> tuple[int, int]:
        """Insert (product_id, url) pairs for a category.

        Returns (inserted, skipped_existing).
        Existing jobs are NOT overwritten (resume-safe).
        """
        inserted = 0
        skipped = 0
        now = time.time()
        with self._txn() as conn:
            for pid, url in items:
                if not pid or not url:
                    continue
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO jobs
                        (product_id, url, category, status, attempts, created_at, updated_at)
                    VALUES (?, ?, ?, 'pending', 0, ?, ?)
                    """,
                    (str(pid), url, category, now, now),
                )
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
        return inserted, skipped

    # ── claim / release ──────────────────────────────────────────────

    def claim_next(
        self,
        worker_id: str,
        category: Optional[str] = None,
    ) -> Optional[dict]:
        """Atomically pick the next pending|needs_refill|stale-claimed job.

        Returns a dict of the claimed job, or None if queue is empty.
        """
        def _attempt():
            now = time.time()
            stale_cutoff = now - CLAIM_STALE_SECONDS
            with self._txn() as conn:
                # Eligible: pending, needs_refill, or claimed but stale
                params: list = [stale_cutoff]
                where = (
                    "(status IN ('pending', 'needs_refill') "
                    " OR (status = 'claimed' AND claimed_at < ?))"
                )
                if category:
                    where += " AND category = ?"
                    params.append(category)
                row = conn.execute(
                    f"SELECT * FROM jobs WHERE {where} "
                    f"ORDER BY status='needs_refill' DESC, created_at ASC LIMIT 1",
                    params,
                ).fetchone()
                if not row:
                    return None
                conn.execute(
                    """
                    UPDATE jobs
                    SET status='claimed', worker_id=?, claimed_at=?,
                        attempts=attempts+1, updated_at=?
                    WHERE product_id=?
                    """,
                    (worker_id, now, now, row["product_id"]),
                )
                return dict(row)
        return _retry_busy(_attempt)

    def mark_done(self, product_id: str):
        now = time.time()
        with self._txn() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status='done', completed_at=?, worker_id=NULL,
                    claimed_at=NULL, last_error=NULL, missing_fields=NULL, updated_at=?
                WHERE product_id=?
                """,
                (now, now, str(product_id)),
            )

    def mark_failed(self, product_id: str, error: str):
        now = time.time()
        with self._txn() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status='failed', last_error=?, worker_id=NULL,
                    claimed_at=NULL, updated_at=?
                WHERE product_id=?
                """,
                (error[:500], now, str(product_id)),
            )

    def mark_needs_refill(self, product_id: str, missing_fields: list[str]):
        """Mark a job as having a partial result that needs completing."""
        now = time.time()
        with self._txn() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status='needs_refill', missing_fields=?, worker_id=NULL,
                    claimed_at=NULL, updated_at=?
                WHERE product_id=?
                """,
                (",".join(missing_fields), now, str(product_id)),
            )

    def release(self, product_id: str):
        """Release a claim without marking outcome (e.g. on Ctrl+C mid-scrape)."""
        now = time.time()
        with self._txn() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status='pending', worker_id=NULL, claimed_at=NULL, updated_at=?
                WHERE product_id=? AND status='claimed'
                """,
                (now, str(product_id)),
            )

    def cleanup_stuck_refills(self, max_attempts: int = MAX_REFILL_ATTEMPTS) -> int:
        """Promote `needs_refill` rows whose attempts already exceed the cap
        to `failed`. Used at startup to rescue items that were stuck looping
        before the cap was introduced. Returns the number of rows promoted.
        """
        now = time.time()
        with self._txn() as conn:
            cur = conn.execute(
                """
                UPDATE jobs
                SET status='failed',
                    last_error=COALESCE(last_error, 'auto-promoted: attempts cap exceeded'),
                    worker_id=NULL,
                    claimed_at=NULL,
                    updated_at=?
                WHERE status='needs_refill' AND attempts >= ?
                """,
                (now, max_attempts),
            )
            return cur.rowcount

    def release_stale(self) -> int:
        """Return claims older than CLAIM_STALE_SECONDS to pending. Returns count."""
        cutoff = time.time() - CLAIM_STALE_SECONDS
        now = time.time()
        with self._txn() as conn:
            cur = conn.execute(
                """
                UPDATE jobs
                SET status='pending', worker_id=NULL, claimed_at=NULL, updated_at=?
                WHERE status='claimed' AND claimed_at < ?
                """,
                (now, cutoff),
            )
            return cur.rowcount

    # ── queries ──────────────────────────────────────────────────────

    def count_claimable(self, category: Optional[str] = None) -> int:
        """Count rows that `claim_next` would currently consider eligible.

        Used to size the worker pool: no point spawning 3 workers if there's
        only 1 item to scrape.
        """
        stale_cutoff = time.time() - CLAIM_STALE_SECONDS
        params: list = [stale_cutoff]
        where = (
            "(status IN ('pending', 'needs_refill') "
            " OR (status = 'claimed' AND claimed_at < ?))"
        )
        if category:
            where += " AND category = ?"
            params.append(category)
        with self._cursor() as cur:
            row = cur.execute(f"SELECT COUNT(*) FROM jobs WHERE {where}", params).fetchone()
        return int(row[0]) if row else 0

    def stats(self, category: Optional[str] = None) -> dict:
        with self._cursor() as cur:
            if category:
                rows = cur.execute(
                    "SELECT status, COUNT(*) FROM jobs WHERE category=? GROUP BY status",
                    (category,),
                ).fetchall()
            else:
                rows = cur.execute(
                    "SELECT status, COUNT(*) FROM jobs GROUP BY status"
                ).fetchall()
        out = {s: 0 for s in VALID_STATUSES}
        for status, count in rows:
            out[status] = count
        out["total"] = sum(out.values())
        return out

    def categories(self) -> list[dict]:
        with self._cursor() as cur:
            rows = cur.execute(
                """
                SELECT category,
                       COUNT(*) AS total,
                       SUM(status='done') AS done,
                       SUM(status='pending') AS pending,
                       SUM(status='claimed') AS claimed,
                       SUM(status='failed') AS failed,
                       SUM(status='needs_refill') AS needs_refill
                FROM jobs
                GROUP BY category
                ORDER BY category
                """
            ).fetchall()
        return [dict(r) for r in rows]

    def get_job(self, product_id: str) -> Optional[dict]:
        with self._cursor() as cur:
            row = cur.execute(
                "SELECT * FROM jobs WHERE product_id=?", (str(product_id),)
            ).fetchone()
        return dict(row) if row else None
