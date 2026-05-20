"""
Completeness check + gap-fill merge for detail JSONs (Phase 2).

A detail is COMPLETE iff every required field passes its validator.
Required fields and rules come from config.REQUIRED_DETAIL_FIELDS.

Merge philosophy
----------------
- STATIC fields (title, brand, modelNumber, itemNumber, gtin, sku, mpn, description,
  descriptionBullets, specifications, images, featureText, featureImages):
    keep old value if it's non-empty; only fill from new if old is empty.
- DYNAMIC fields (price, wasPrice, rating, reviewCount):
    always take new value if non-empty (these change over time).
- Dict fields (specifications): merged key-by-key — new keys added, old keys preserved.
- List fields (images, featureImages, descriptionBullets, featureText):
    union, preserving old order first, dedup case-sensitively.
"""
from __future__ import annotations

from typing import Any

from config import REQUIRED_DETAIL_FIELDS


# Fields whose value reflects the current state of the world.
# These should always be refreshed from a new scrape if available.
DYNAMIC_FIELDS = {"price", "wasPrice", "rating", "reviewCount"}

# Fields stored as dicts — merge key-by-key
DICT_FIELDS = {"specifications"}

# Fields stored as lists — merge by union+dedup, preserving old order first
LIST_FIELDS = {"images", "featureImages", "descriptionBullets", "featureText"}


# ─── Validators ──────────────────────────────────────────────────────────

def _non_empty_str(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())


def _list_min_1(v: Any) -> bool:
    return isinstance(v, list) and len(v) >= 1


def _dict_min_5(v: Any) -> bool:
    return isinstance(v, dict) and len(v) >= 5


def _dict_min_3(v: Any) -> bool:
    return isinstance(v, dict) and len(v) >= 3


_VALIDATORS = {
    "non_empty_str": _non_empty_str,
    "list_min_1": _list_min_1,
    "dict_min_3": _dict_min_3,
    "dict_min_5": _dict_min_5,
}


# ─── Public API ──────────────────────────────────────────────────────────

def check_completeness(detail: dict | None) -> tuple[bool, list[str]]:
    """Return (is_complete, missing_fields).

    missing_fields is a list of REQUIRED field names whose values failed
    their validator. Empty list means complete.
    """
    if not isinstance(detail, dict):
        return False, list(REQUIRED_DETAIL_FIELDS.keys())
    missing: list[str] = []
    for field, rule in REQUIRED_DETAIL_FIELDS.items():
        validator = _VALIDATORS.get(rule)
        if validator is None:
            # Unknown rule => permissive: treat as present if key exists
            if not detail.get(field):
                missing.append(field)
            continue
        if not validator(detail.get(field)):
            missing.append(field)
    return (len(missing) == 0), missing


def merge_detail(old: dict | None, new: dict) -> dict:
    """Merge `new` scrape result into `old` according to the merge rules.

    - Static fields: prefer old (non-empty)
    - Dynamic fields: prefer new (non-empty)
    - Dict fields (specifications): key-by-key merge, prefer old per key
    - List fields: union dedup, old order first
    Returns a NEW dict — does not mutate inputs.
    """
    if not old:
        # Nothing to merge — just return a shallow copy of new
        return dict(new)
    out = dict(old)
    for key, new_val in new.items():
        if key in DICT_FIELDS:
            old_val = out.get(key) or {}
            if not isinstance(old_val, dict):
                old_val = {}
            if not isinstance(new_val, dict):
                continue
            merged = dict(old_val)
            for k, v in new_val.items():
                # Prefer old value if it's already there and non-empty
                if not merged.get(k):
                    if v:
                        merged[k] = v
            out[key] = merged
        elif key in LIST_FIELDS:
            old_list = out.get(key) or []
            if not isinstance(old_list, list):
                old_list = []
            if not isinstance(new_val, list):
                continue
            seen = set()
            merged_list = []
            for item in list(old_list) + list(new_val):
                # Use a stable hashable key for dedup
                k = item if isinstance(item, (str, int, float, bool)) else repr(item)
                if k in seen:
                    continue
                seen.add(k)
                merged_list.append(item)
            out[key] = merged_list
        elif key in DYNAMIC_FIELDS:
            # Always update if new has a non-empty value
            if new_val not in (None, "", [], {}):
                out[key] = new_val
        else:
            # Static field: only fill if old is empty
            if not out.get(key) and new_val not in (None, "", [], {}):
                out[key] = new_val
    # Refresh metadata
    if "scraped_at" in new:
        out["scraped_at"] = new["scraped_at"]
    return out
