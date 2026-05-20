"""
Live proxy connectivity test.

Issues a request to a public IP-echo service through the proxy and returns:
{ok, ip, country?, rtt_ms, error?}

Used by Settings UI and by profile_setup before binding a proxy to AdsPower.
"""
from __future__ import annotations

import time
from typing import Optional

import requests

from proxy_parser import Proxy, to_requests_dict

# Public IP echo services (try in order)
ECHO_ENDPOINTS = [
    "https://api.ipify.org?format=json",
    "https://ifconfig.me/all.json",
    "https://ipinfo.io/json",
]

DEFAULT_TIMEOUT = 12  # seconds


def test_proxy(proxy: Proxy, timeout: int = DEFAULT_TIMEOUT) -> dict:
    """Connect through `proxy`, return diagnostics dict.

    Result schema:
      {
        "ok": bool,
        "ip": str | None,        # detected egress IP
        "country": str | None,
        "rtt_ms": int | None,
        "endpoint": str | None,  # which echo service answered
        "error": str | None,
      }
    """
    proxies = to_requests_dict(proxy)

    # SOCKS support requires 'requests[socks]' (PySocks). Detect and report cleanly.
    if proxy["scheme"].startswith("socks"):
        try:
            import socks  # noqa: F401
        except ImportError:
            return {
                "ok": False,
                "ip": None,
                "country": None,
                "rtt_ms": None,
                "endpoint": None,
                "error": (
                    "SOCKS support requires PySocks. "
                    "Install with: pip install requests[socks]"
                ),
            }

    last_err: Optional[str] = None
    for url in ECHO_ENDPOINTS:
        start = time.perf_counter()
        try:
            r = requests.get(url, proxies=proxies, timeout=timeout)
            rtt_ms = int((time.perf_counter() - start) * 1000)
            if r.status_code != 200:
                last_err = f"{url} → HTTP {r.status_code}"
                continue
            data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            ip = (
                data.get("ip")
                or data.get("ip_addr")
                or data.get("origin")
                or r.text.strip()
            )
            country = data.get("country") or data.get("country_code")
            return {
                "ok": True,
                "ip": ip,
                "country": country,
                "rtt_ms": rtt_ms,
                "endpoint": url,
                "error": None,
            }
        except requests.exceptions.ProxyError as e:
            last_err = f"proxy error: {e}"
        except requests.exceptions.ConnectTimeout:
            last_err = f"connect timeout after {timeout}s"
        except requests.exceptions.ReadTimeout:
            last_err = f"read timeout after {timeout}s"
        except requests.exceptions.ConnectionError as e:
            last_err = f"connection error: {e}"
        except Exception as e:  # noqa: BLE001
            last_err = f"{type(e).__name__}: {e}"
    return {
        "ok": False,
        "ip": None,
        "country": None,
        "rtt_ms": None,
        "endpoint": None,
        "error": last_err or "all echo endpoints failed",
    }


def test_proxy_string(raw: str) -> dict:
    """Parse + test in one shot. Returns the same shape as `test_proxy`, plus 'proxy'."""
    from proxy_parser import parse_proxy
    try:
        p = parse_proxy(raw)
    except ValueError as e:
        return {
            "ok": False, "ip": None, "country": None, "rtt_ms": None,
            "endpoint": None, "error": f"parse error: {e}", "proxy": None,
        }
    res = test_proxy(p)
    res["proxy"] = {k: v for k, v in p.items() if k != "password"}
    return res
