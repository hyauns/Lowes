"""
Worker → profile/proxy bootstrap.

Given a (worker_index, profile_id_or_empty, proxy_string) trio, ensure that:
  1. The proxy string parses and (optionally) responds.
  2. A matching AdsPower profile exists. If profile_id_or_empty is empty OR
     points to a non-existent profile, create a fresh one and bind the proxy.
  3. If the profile exists but its proxy differs from the desired one,
     update it.
  4. Return the ready `user_id` plus diagnostics.

Used at runner-start time so workers don't race in the middle of a scrape.
"""
from __future__ import annotations

import time

from adspower_helper import (
    create_profile,
    find_profile,
    update_proxy,
    list_groups,
    find_group_id,
)
from proxy_parser import parse_proxy, to_adspower_user_proxy_config
from proxy_tester import test_proxy


def _proxy_equals(existing: dict | None, desired: dict) -> bool:
    if not isinstance(existing, dict):
        return False
    keys = ("proxy_type", "proxy_host", "proxy_port", "proxy_user", "proxy_password")
    return all(str(existing.get(k, "")) == str(desired.get(k, "")) for k in keys)


def setup_worker_profile(
    api_url: str,
    worker_index: int,
    profile_id: str | None,
    proxy_raw: str,
    verify_proxy: bool = True,
    create_group_name: str = "Scraper",
    fallback_to_local: bool = False,
) -> dict:
    """Bootstrap one worker. Idempotent.

    Returns:
      {
        "worker": int,
        "profile_id": str,           # ready-to-use AdsPower user_id
        "created": bool,             # True if we just created it
        "proxy_updated": bool,       # True if we patched the proxy
        "ip": str | None,            # current egress IP (if verified)
        "proxy_ok": bool,
        "proxy_error": str | None,
        "using_local": bool,         # True if profile bound to local network
      }

    Phase 5.3:
      • `fallback_to_local=True` → if the proxy is unreachable, bind the
        profile to local network (noproxy) instead of raising. Used by the
        orchestrator after the 120s revive wait still finds it dead.
      • `fallback_to_local=False` (default) → original behavior: raise
        ValueError when the proxy is dead. Settings-UI "Setup worker" still
        uses this to surface clear errors.

    Raises ValueError if proxy_raw is invalid (or unreachable + no fallback).
    """
    if not proxy_raw or not proxy_raw.strip():
        if fallback_to_local:
            return _build_local_only_profile(api_url, worker_index, profile_id, create_group_name)
        raise ValueError(
            f"Worker {worker_index}: proxy is required (config.PROXIES[{worker_index}] is empty)"
        )

    # 1) Parse proxy
    proxy = parse_proxy(proxy_raw)
    desired_cfg = to_adspower_user_proxy_config(proxy)

    # 2) Optional liveness test BEFORE binding to AdsPower
    proxy_ok = True
    ip = None
    proxy_error = None
    if verify_proxy:
        t = test_proxy(proxy)
        proxy_ok = t["ok"]
        ip = t.get("ip")
        proxy_error = t.get("error")
        if not proxy_ok:
            if fallback_to_local:
                print(
                    f"[profile_setup] worker {worker_index}: proxy unreachable "
                    f"({proxy_error}) — falling back to local network."
                )
                return _build_local_only_profile(
                    api_url, worker_index, profile_id, create_group_name,
                    proxy_error=proxy_error,
                )
            raise ValueError(
                f"Worker {worker_index}: proxy unreachable ({proxy_error}). "
                f"Fix PROXIES[{worker_index}] before continuing."
            )

    # 3) Find or create the profile
    created = False
    proxy_updated = False
    existing = find_profile(api_url, profile_id) if profile_id else None

    if not existing:
        # Need to create. Pick group.
        gid = find_group_id(api_url, create_group_name)
        if not gid:
            groups = list_groups(api_url)
            if not groups:
                raise RuntimeError(
                    "AdsPower has no groups. Create a group called 'Scraper' in the AdsPower UI."
                )
            gid = str(groups[0]["group_id"])
        name = f"lowes-w{worker_index}-{int(time.time())}"
        # Rotate desktop OS across workers for fingerprint diversity.
        # NEVER mobile (iOS/Android) — Lowes serves a different DOM that breaks
        # our scraper. create_profile() enforces this hard.
        desktop_os = ("windows", "mac", "linux")[worker_index % 3]
        new_id = create_profile(
            api_url,
            name=name,
            proxy_config=desired_cfg,
            group_id=gid,
            os_type=desktop_os,
        )
        created = True
        profile_id = new_id
    else:
        # Compare and patch proxy if needed
        if not _proxy_equals(existing.get("user_proxy_config"), desired_cfg):
            update_proxy(api_url, profile_id, desired_cfg)
            proxy_updated = True

    return {
        "worker": worker_index,
        "profile_id": profile_id,
        "created": created,
        "proxy_updated": proxy_updated,
        "ip": ip,
        "proxy_ok": proxy_ok,
        "proxy_error": proxy_error,
        "using_local": False,
        "proxy": {  # echo back without password
            "scheme": proxy["scheme"],
            "host": proxy["host"],
            "port": proxy["port"],
            "user": proxy.get("user"),
        },
    }


def _build_local_only_profile(
    api_url: str,
    worker_index: int,
    profile_id: str | None,
    create_group_name: str,
    proxy_error: str | None = None,
) -> dict:
    """Create-or-rebind an AdsPower profile to local network (noproxy).

    Used by setup_worker_profile when fallback_to_local=True and the configured
    proxy is dead. Profile fingerprint stays desktop-only.
    """
    # AdsPower no-proxy payload: provider id is "no_proxy" (with underscore).
    # The "proxy_type":"noproxy" shape is rejected by /user/create with
    # "user_proxy_config incorrect format".
    desired_cfg = {"proxy_soft": "no_proxy"}
    created = False
    proxy_updated = False
    existing = find_profile(api_url, profile_id) if profile_id else None

    if not existing:
        gid = find_group_id(api_url, create_group_name)
        if not gid:
            groups = list_groups(api_url)
            if not groups:
                raise RuntimeError(
                    "AdsPower has no groups. Create a 'Scraper' group in the UI."
                )
            gid = str(groups[0]["group_id"])
        name = f"lowes-w{worker_index}-local-{int(time.time())}"
        desktop_os = ("windows", "mac", "linux")[worker_index % 3]
        new_id = create_profile(
            api_url,
            name=name,
            proxy_config=desired_cfg,
            group_id=gid,
            os_type=desktop_os,
        )
        created = True
        profile_id = new_id
    else:
        if not _proxy_equals(existing.get("user_proxy_config"), desired_cfg):
            update_proxy(api_url, profile_id, desired_cfg)
            proxy_updated = True

    return {
        "worker": worker_index,
        "profile_id": profile_id,
        "created": created,
        "proxy_updated": proxy_updated,
        "ip": "local",
        "proxy_ok": False,
        "proxy_error": proxy_error,
        "using_local": True,
        "proxy": {"scheme": "noproxy", "host": None, "port": None, "user": None},
    }


def setup_all_workers(
    api_url: str,
    profiles: list[str],
    proxies: list[str],
    worker_count: int,
    verify_proxy: bool = True,
    revive_wait_seconds: int = 120,
) -> list[dict]:
    """Bootstrap N workers.

    Phase 5.3:
      • Test all configured proxies first. If any are dead, sleep
        `revive_wait_seconds` (default 120s) and re-test ONCE. Per user:
        proxyxoay-style proxies always come back, just slow to rotate.
      • Workers whose proxy is STILL dead after the revive wait are
        bootstrapped onto local network (noproxy). They'll run on the
        local IP until they hit a block, then swap_to_alive_proxy fires.
      • Duplicate proxy strings are allowed (warn, don't raise).
    """
    if worker_count < 1:
        raise ValueError("worker_count must be >= 1")
    if len(profiles) < worker_count:
        raise ValueError(
            f"PROFILES has {len(profiles)} entries, need {worker_count}. "
            "Either lower WORKER_COUNT or add empty strings to PROFILES."
        )
    if len(proxies) < worker_count:
        raise ValueError(
            f"PROXIES has {len(proxies)} entries, need {worker_count}. "
            "Pad with empty strings or lower WORKER_COUNT."
        )
    # Phase 5.3: duplicate proxy strings are allowed now (proxyxoay-style
    # rotating proxies don't actually share an egress IP between callers).
    # We warn so the user sees it but don't refuse to start.
    seen: dict[str, int] = {}
    for i, p in enumerate(proxies[:worker_count]):
        if not p.strip():
            continue
        if p in seen:
            print(
                f"[profile_setup] note: Proxies[{i}] duplicates Proxies[{seen[p]}] "
                f"— allowed (rotating-proxy assumption)."
            )
        else:
            seen[p] = i

    # Phase 5.3 pre-flight proxy health check
    if verify_proxy:
        proxy_alive = _preflight_proxy_health(
            proxies[:worker_count],
            revive_wait_seconds,
        )
    else:
        proxy_alive = [True] * worker_count  # caller said "trust me"

    results = []
    for i in range(worker_count):
        # Phase 5.4 resilience: a per-worker bootstrap failure (e.g. AdsPower
        # rejects a payload, profile id no longer exists, etc.) must NOT crash
        # the whole orchestrator. Record the failure and let the remaining
        # workers proceed. Orchestrator.run_details skips errored entries.
        try:
            if proxy_alive[i]:
                # Skip per-worker proxy re-test (pre-flight already confirmed)
                r = setup_worker_profile(
                    api_url,
                    worker_index=i,
                    profile_id=profiles[i] or None,
                    proxy_raw=proxies[i],
                    verify_proxy=False,
                    fallback_to_local=True,
                )
            else:
                print(
                    f"[profile_setup] worker {i}: proxy dead after pre-flight "
                    f"— booting on local network."
                )
                r = _build_local_only_profile(
                    api_url, i, profiles[i] or None, "Scraper",
                    proxy_error="dead at pre-flight",
                )
            results.append(r)
        except Exception as e:
            print(
                f"[profile_setup] worker {i}: bootstrap FAILED ({e}) — "
                f"recording as errored so other workers can still run."
            )
            results.append({
                "worker": i,
                "profile_id": None,
                "created": False,
                "proxy_updated": False,
                "ip": None,
                "proxy_ok": False,
                "proxy_error": str(e)[:200],
                "using_local": False,
                "bootstrap_error": str(e)[:200],
                "proxy": {"scheme": None, "host": None, "port": None, "user": None},
            })

    return results


def _preflight_proxy_health(
    proxies: list[str],
    revive_wait_seconds: int,
) -> list[bool]:
    """Return per-slot alive flag. Sleeps once if any are dead on first pass."""
    def _scan() -> list[bool]:
        flags: list[bool] = []
        for i, raw in enumerate(proxies):
            if not raw or not raw.strip():
                flags.append(False)
                continue
            try:
                p = parse_proxy(raw)
                r = test_proxy(p)
                ok = bool(r.get("ok"))
                ip = r.get("ip", "?")
                print(
                    f"[proxy-check] slot[{i}]: {'OK' if ok else 'DEAD'} "
                    f"({ip if ok else r.get('error', '')})"
                )
                flags.append(ok)
            except Exception as e:
                print(f"[proxy-check] slot[{i}]: parse/test failed — {e}")
                flags.append(False)
        return flags

    flags = _scan()
    if all(flags) or not any(raw and raw.strip() for raw in proxies):
        return flags

    # Phase 5.4 update: only block the full revive wait if EVERY proxy is dead.
    # If at least one is alive, workers with alive proxies must boot immediately
    # — per user: "1 trong 3 worker bị proxy dead thì 2 worker còn lại vẫn phải
    # làm việc". Dead-slot workers fall straight through to local network
    # (setup_all_workers handles this).
    if not all(not f for f in flags):
        dead = [i for i, f in enumerate(flags) if not f]
        print(
            f"[proxy-check] Dead slots {dead} — skipping revive wait so the "
            f"alive-proxy workers can boot now. Dead-slot workers will start "
            f"on local network and self-recover when their proxy comes back."
        )
        return flags

    print(
        f"[proxy-check] All proxies dead on first pass — "
        f"waiting {revive_wait_seconds}s for rotation, then testing ONCE more..."
    )
    time.sleep(revive_wait_seconds)
    # Re-scan; only flip dead→alive, never alive→dead (don't retest live ones).
    flags2 = _scan()
    return [a or b for a, b in zip(flags, flags2)]
