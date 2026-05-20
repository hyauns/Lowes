"""
Phase 5.4 — proxyxoay rotation API client.

Hits  GET https://proxyxoay.net/api/rotating-proxy/change-key-ip/<api_key>

Behavior observed in live tests (2026-05-17):
  • HTTP 200 + {"status":200,"message":"Thay đổi IP thành công, vui lòng đợi vài giây"}
      → Rotation accepted. New IP may take ~10s+ to propagate. Sometimes the
        egress IP doesn't actually change (provider-side pool quirk) — caller
        should proceed anyway, the new profile may still get a fresh fingerprint.
  • HTTP 400 + {"status":400,"message":"Vui lòng chờ sau N giây"}
      → Per-key cooldown. We parse N and wait N+1s, then retry ONCE. If N
        exceeds PROXY_ROTATION_MAX_WAIT, give up and let caller fall back.
"""
from __future__ import annotations

import re
import time
from typing import Optional

import requests

from config import (
    PROXY_ROTATION_API_BASE,
    PROXY_ROTATION_MAX_WAIT,
    PROXY_ALIVE_WAIT_INTERVAL,
    PROXY_ALIVE_WAIT_MAX_ATTEMPTS,
)
from proxy_parser import parse_proxy, to_requests_dict
from proxy_tester import test_proxy

# Matches "Vui lòng chờ sau 97 giây" / "chờ sau 12 giay" etc.
_COOLDOWN_RE = re.compile(r"sau\s+(\d+)\s+gi[aâ]y", re.IGNORECASE)

# Egress-IP echo (cheap public service). 12s timeout — proxy may be slow.
_IPIFY = "https://api.ipify.org?format=json"
_ECHO_TIMEOUT = 12


def _current_egress_ip(proxy_raw: str) -> Optional[str]:
    """Return current egress IP via the given proxy, or None on failure."""
    try:
        p = parse_proxy(proxy_raw)
        proxies = to_requests_dict(p)
        r = requests.get(_IPIFY, proxies=proxies, timeout=_ECHO_TIMEOUT)
        if r.ok:
            return r.json().get("ip")
    except Exception:
        return None
    return None


def _parse_cooldown_seconds(msg: str) -> Optional[int]:
    m = _COOLDOWN_RE.search(msg or "")
    return int(m.group(1)) if m else None


def rotate_proxy_ip(
    api_key: str,
    proxy_raw: Optional[str] = None,
    max_cooldown_wait: int = PROXY_ROTATION_MAX_WAIT,
    alive_wait_interval: int = PROXY_ALIVE_WAIT_INTERVAL,
    alive_wait_max_attempts: int = PROXY_ALIVE_WAIT_MAX_ATTEMPTS,
    log_prefix: str = "",
) -> dict:
    """Call the proxyxoay rotation endpoint and (optionally) poll the new IP.

    Args:
        api_key: proxyxoay rotation key (from config.PROXY_ROTATION_KEYS).
        proxy_raw: the proxy URL string. If provided, we measure the old IP
                   before the call and poll the new IP after. If None, we just
                   fire the rotation and return whatever the API said.
        max_cooldown_wait: HTTP 400 cooldowns longer than this give up.
        poll_timeout: max seconds to poll for IP change after HTTP 200.
        log_prefix: e.g. "  [w0] [rotate]" — prepended to print() lines.

    Returns:
        {
          "ok": bool,                  # True if rotation was accepted (200)
          "old_ip": str | None,
          "new_ip": str | None,        # last observed IP (may equal old_ip)
          "ip_changed": bool,
          "message": str,              # from API or local error
          "cooldown_waited": int,      # seconds slept due to HTTP 400
          "http_status": int | None,
        }
    """
    url = f"{PROXY_ROTATION_API_BASE.rstrip('/')}/{api_key}"
    old_ip = _current_egress_ip(proxy_raw) if proxy_raw else None
    if log_prefix:
        print(f"{log_prefix} rotate: old IP {old_ip or 'unknown'}")

    cooldown_waited = 0

    # ── First attempt ────────────────────────────────────────────────────
    try:
        r = requests.get(url, timeout=20)
    except Exception as e:
        return _fail(old_ip, f"request failed: {e}", 0, None)

    try:
        j = r.json()
    except Exception:
        j = {"status": r.status_code, "message": (r.text or "")[:200]}

    # ── Cooldown branch ──────────────────────────────────────────────────
    if r.status_code == 400:
        msg = j.get("message") or ""
        wait_s = _parse_cooldown_seconds(msg)
        if wait_s is None:
            # Unknown 400 — just report and bail.
            return _fail(old_ip, f"HTTP 400: {msg}", 0, 400)
        if wait_s > max_cooldown_wait:
            return _fail(
                old_ip,
                f"cooldown {wait_s}s exceeds max wait {max_cooldown_wait}s",
                0, 400,
            )
        if log_prefix:
            print(f"{log_prefix} rotate: cooldown {wait_s}s — waiting then retrying once")
        time.sleep(wait_s + 1)
        cooldown_waited = wait_s + 1
        try:
            r = requests.get(url, timeout=20)
            j = r.json()
        except Exception as e:
            return _fail(old_ip, f"retry after cooldown failed: {e}",
                         cooldown_waited, None)

    # ── Now expect 200 ───────────────────────────────────────────────────
    if r.status_code != 200 or j.get("status") != 200:
        return _fail(
            old_ip,
            f"unexpected response after rotation: HTTP {r.status_code} body={j}",
            cooldown_waited, r.status_code,
        )

    api_msg = j.get("message", "ok")
    if log_prefix:
        print(f"{log_prefix} rotate: API accepted ({api_msg})")

    # ── Poll for new IP if we know the proxy ─────────────────────────────
    if not proxy_raw:
        return {
            "ok": True, "old_ip": old_ip, "new_ip": None, "ip_changed": False,
            "message": api_msg, "cooldown_waited": cooldown_waited,
            "http_status": 200,
        }

    # Wait until proxy is ACTUALLY ALIVE before returning success. Per user
    # (2026-05-17 follow-up): "Nếu worker xoay proxy qua API xong, mà proxy
    # chưa khởi động kịp lại (Dead) hãy chờ 60s kiểm tra proxy 1 lần cho tới
    # khi proxy live thì bắt đầu làm việc lại."
    #
    # Caller (rotate_and_recreate) blocks here until alive, then creates the
    # new profile. Hard cap = alive_wait_max_attempts × alive_wait_interval
    # (default 30 × 60s = 30 min) so we don't hang forever if the provider
    # is completely down. If we hit the cap, return ok=False so the caller
    # can fall back to swap_to_alive_proxy (different slot) or local network.
    new_ip = old_ip
    try:
        proxy_obj = parse_proxy(proxy_raw)
    except Exception as e:
        return _fail(old_ip, f"proxy parse failed during alive-wait: {e}",
                     cooldown_waited, 200)

    for attempt in range(1, alive_wait_max_attempts + 1):
        time.sleep(alive_wait_interval)
        t = test_proxy(proxy_obj)
        ip = t.get("ip")
        if t.get("ok"):
            new_ip = ip or new_ip
            if log_prefix:
                ip_changed_now = new_ip != old_ip
                tag = "new IP" if ip_changed_now else "same IP"
                print(
                    f"{log_prefix} rotate: alive after {attempt}×{alive_wait_interval}s "
                    f"({tag} {new_ip})"
                )
            return {
                "ok": True, "old_ip": old_ip, "new_ip": new_ip,
                "ip_changed": (new_ip != old_ip),
                "message": api_msg, "cooldown_waited": cooldown_waited,
                "http_status": 200, "alive_wait_attempts": attempt,
            }
        if log_prefix:
            print(
                f"{log_prefix} rotate: proxy still DEAD on attempt "
                f"{attempt}/{alive_wait_max_attempts} — waiting another "
                f"{alive_wait_interval}s ({t.get('error', '?')})"
            )

    # Hit cap without seeing alive — surface as failure so caller falls back.
    return _fail(
        old_ip,
        f"proxy still dead after {alive_wait_max_attempts}×{alive_wait_interval}s",
        cooldown_waited, 200,
    )


def _fail(old_ip, message, cooldown_waited, http_status) -> dict:
    return {
        "ok": False, "old_ip": old_ip, "new_ip": old_ip, "ip_changed": False,
        "message": message, "cooldown_waited": cooldown_waited,
        "http_status": http_status,
    }
