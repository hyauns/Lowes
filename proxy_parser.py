"""
Proxy string parser.

Accepts several common formats and normalizes them into a `Proxy` dict.

Supported inputs
----------------
1. Standard URL    : scheme://user:pass@host:port
2. Swapped URL     : scheme://host:port@user:pass     (sometimes given by proxy sellers)
3. Colon-separated : host:port:user:pass              (optional scheme:// prefix)
4. No auth         : scheme://host:port  OR  host:port

`scheme` is one of: http, https, socks5. Defaults to http if omitted.

Output shape
------------
{
  "scheme":   "http" | "https" | "socks5",
  "host":     str,
  "port":     int,
  "user":     str | None,
  "password": str | None,
  "url":      "scheme://[user:pass@]host:port"   (rebuilt canonical form)
}

Raises ValueError on inputs that cannot be parsed.
"""
from __future__ import annotations

import re
from typing import TypedDict, Optional


class Proxy(TypedDict, total=False):
    scheme: str
    host: str
    port: int
    user: Optional[str]
    password: Optional[str]
    url: str


VALID_SCHEMES = ("http", "https", "socks5", "socks5h")
_HOST_PORT_RE = re.compile(r"^([^\s:@]+):(\d{1,5})$")


def parse_proxy(raw: str) -> Proxy:
    if not raw or not isinstance(raw, str):
        raise ValueError("empty proxy string")
    raw = raw.strip()

    # Pull scheme off the front if present
    scheme = "http"
    if "://" in raw:
        scheme_part, _, rest = raw.partition("://")
        scheme_part = scheme_part.strip().lower()
        if scheme_part not in VALID_SCHEMES:
            raise ValueError(f"unsupported scheme '{scheme_part}'. Use one of {VALID_SCHEMES}")
        scheme = scheme_part
    else:
        rest = raw

    user: Optional[str] = None
    password: Optional[str] = None
    host: str
    port: int

    # Case A: contains '@' — could be standard or swapped form
    if "@" in rest:
        left, _, right = rest.partition("@")
        # If right side parses as host:port → standard form (left = user:pass)
        m_right = _HOST_PORT_RE.match(right)
        m_left = _HOST_PORT_RE.match(left)
        if m_right and not m_left:
            # standard: user:pass@host:port
            host = m_right.group(1)
            port = int(m_right.group(2))
            if ":" in left:
                user, _, password = left.partition(":")
            else:
                user = left
        elif m_left and not m_right:
            # swapped: host:port@user:pass  (user's format)
            host = m_left.group(1)
            port = int(m_left.group(2))
            if ":" in right:
                user, _, password = right.partition(":")
            else:
                user = right
        elif m_right and m_left:
            # Both parse as host:port — assume standard (user:pass@host:port)
            host = m_right.group(1)
            port = int(m_right.group(2))
            if ":" in left:
                user, _, password = left.partition(":")
            else:
                user = left
        else:
            raise ValueError(f"cannot locate host:port in '{raw}'")
    else:
        # Case B: no '@' — either host:port or host:port:user:pass
        parts = rest.split(":")
        if len(parts) == 2:
            host = parts[0]
            try:
                port = int(parts[1])
            except ValueError as e:
                raise ValueError(f"bad port in '{raw}'") from e
        elif len(parts) == 4:
            host = parts[0]
            try:
                port = int(parts[1])
            except ValueError as e:
                raise ValueError(f"bad port in '{raw}'") from e
            user = parts[2]
            password = parts[3]
        else:
            raise ValueError(
                f"unrecognised proxy format '{raw}'. "
                "Expected scheme://user:pass@host:port, scheme://host:port@user:pass, "
                "or host:port[:user:pass]"
            )

    if not host:
        raise ValueError("missing host")
    if not (1 <= port <= 65535):
        raise ValueError(f"port {port} out of range")

    # Rebuild canonical URL (standard form)
    if user is not None:
        auth = user if password is None else f"{user}:{password}"
        url = f"{scheme}://{auth}@{host}:{port}"
    else:
        url = f"{scheme}://{host}:{port}"

    return Proxy(
        scheme=scheme,
        host=host,
        port=port,
        user=user,
        password=password,
        url=url,
    )


def to_requests_dict(proxy: Proxy) -> dict:
    """Build the dict expected by `requests` (and `httpx`) for HTTP+HTTPS routing."""
    return {"http": proxy["url"], "https": proxy["url"]}


def to_adspower_user_proxy_config(proxy: Proxy) -> dict:
    """Build AdsPower /api/v1/user/create user_proxy_config payload."""
    return {
        "proxy_soft": "other",
        "proxy_type": proxy["scheme"],   # http / https / socks5
        "proxy_host": proxy["host"],
        "proxy_port": str(proxy["port"]),
        "proxy_user": proxy.get("user") or "",
        "proxy_password": proxy.get("password") or "",
    }
