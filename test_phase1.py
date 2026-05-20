"""Smoke test for Phase 1 state manager. Run with: python test_phase1.py"""
import os
import time

# Use a throw-away DB so we don't touch real data
TEST_DB = "data/_test_state.db"
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)
for sfx in ("-wal", "-shm"):
    p = TEST_DB + sfx
    if os.path.exists(p):
        os.remove(p)

# Inject test DB path before importing state_manager
import config  # noqa: E402
config.STATE_DB_PATH = TEST_DB
config.STATE_DB_BACKUP = TEST_DB + ".bak"
config.CLAIM_STALE_SECONDS = 2  # tiny for staleness test

from state_manager import StateManager  # noqa: E402


def t(label, cond):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {label}")
    assert cond, label


def main():
    print("=== Phase 1 smoke test ===")
    sm = StateManager()

    print("\n1) Enqueue")
    ins, skp = sm.enqueue_listing("test-cat", [
        ("p1", "https://x/p1"),
        ("p2", "https://x/p2"),
        ("p3", "https://x/p3"),
    ])
    t("3 new jobs inserted", ins == 3 and skp == 0)

    print("\n2) Idempotent enqueue (resume safety)")
    ins, skp = sm.enqueue_listing("test-cat", [
        ("p1", "https://x/p1"),
        ("p4", "https://x/p4"),
    ])
    t("1 new, 1 skipped", ins == 1 and skp == 1)

    print("\n3) Atomic claim — two workers don't collide")
    j_a = sm.claim_next("wA", category="test-cat")
    j_b = sm.claim_next("wB", category="test-cat")
    t("worker A got a job", j_a is not None)
    t("worker B got a different job", j_b is not None and j_b["product_id"] != j_a["product_id"])

    print("\n4) mark_done / stats")
    sm.mark_done(j_a["product_id"])
    s = sm.stats("test-cat")
    t("1 done", s["done"] == 1)
    t("1 claimed (worker B)", s["claimed"] == 1)
    t("2 pending", s["pending"] == 2)

    print("\n5) release_stale (simulate worker crash)")
    print("   waiting 2.5s for claim to go stale...")
    time.sleep(2.5)
    n = sm.release_stale()
    t("released >=1 stale claim", n >= 1)
    s = sm.stats("test-cat")
    t("3 pending after release", s["pending"] == 3)

    print("\n6) mark_failed")
    j = sm.claim_next("wC", category="test-cat")
    sm.mark_failed(j["product_id"], "boom")
    s = sm.stats("test-cat")
    t("1 failed", s["failed"] == 1)

    print("\n7) needs_refill flow")
    j = sm.claim_next("wD", category="test-cat")
    sm.mark_needs_refill(j["product_id"], ["specifications", "images"])
    s = sm.stats("test-cat")
    t("1 needs_refill", s["needs_refill"] == 1)
    # refill jobs should be re-claimable, prioritized
    j2 = sm.claim_next("wD", category="test-cat")
    t("refill job re-claimed", j2 is not None and j2["product_id"] == j["product_id"])

    print("\n8) release() restores pending")
    sm.release(j2["product_id"])
    job = sm.get_job(j2["product_id"])
    t("status back to pending", job["status"] == "pending")

    print("\n9) categories() summary")
    cats = sm.categories()
    t("one category row", len(cats) == 1 and cats[0]["category"] == "test-cat")

    sm.close()
    print("\n=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    main()
