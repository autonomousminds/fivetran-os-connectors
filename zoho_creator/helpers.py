"""
Shared utilities for the Zoho Creator connector — config validation, record
flattening, upsert wrapper, hard-delete reconciliation, full-sync scheduling.

Flattening rules:
  - **System fields** (`ID`, `Added_Time`, `Modified_Time`, `Added_User`,
    `Modified_User`, `zc_display_value`) → pass through.
  - **Lookup field** (dict with `ID` and a display value) → exploded into
    `{field}_id` + `{field}_display_value`.
  - **Address dict** (sub-keys like `district_city`, `state_province`, …) →
    flattened with `{field}_{sub}` prefix.
  - **File upload** (dict with `filepath`/`url` and no `ID`) → stored as
    `{field}_url` + `{field}_filename`. Binaries are NOT downloaded.
  - **Subform** (list of dicts) → DROPPED from the parent; handled separately
    by the caller (extract into a child table).
  - **Multi-select / array of primitives or non-ID dicts** → JSON-encoded into
    one column.
  - Everything else → JSON-encoded for unfamiliar dicts.
"""

import json
import re
import time
from collections import defaultdict

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

STATE_VERSION = "5"  # bumped: Data API default — full column set, not bulk-read's lean report-view


_COL_SAFE_RE = re.compile(r"[^A-Za-z0-9_]+")


def safe_column_name(name: str) -> str:
    """Lower-case, alphanumeric/underscore only.

    Two things this fixes:

      1. **Dots, periods, parentheses, etc.** — Zoho returns column names
         like `Contacts.Letter_of_Authority` for denormalised lookups
         across forms; BigQuery rejects dots, Snowflake quotes them
         awkwardly. We collapse all non-alphanumeric to `_`.

      2. **Case collisions.** Zoho's Bulk Read CSV sometimes returns the
         SAME lookup field as both `Client_Title.id` and `Client_Title_ID`
         (one is the denormalised FK, the other is a field on the related
         record). Python keeps them as separate dict keys, but DuckDB and
         Snowflake treat identifiers case-insensitively and refuse a
         CREATE TABLE that mentions both. Lower-casing here merges them
         into a single column, and `flatten_record_auto` resolves the
         per-row value collision by preferring non-null/non-empty.
    """
    cleaned = _COL_SAFE_RE.sub("_", name).lower()
    return cleaned.strip("_") or "col"


def safe_table_suffix(name: str) -> str:
    """Lower-snake-case, alphanumeric only. Used for subform child-table
    names where Zoho subform field names can contain dots (e.g.
    `Contact.Name` produces `contact_name` rather than `contact.name`)."""
    return _COL_SAFE_RE.sub("_", name.lower()).strip("_") or "sub"

# How often a report should re-run its Bulk Read full-sync (to catch hard-deletes
# the Modified_Time filter misses).
FULL_SYNC_INTERVAL_SECONDS = 7 * 24 * 3600

# Per-column orphan tracking (lookup column → referenced IDs vs IDs we wrote).
_ids_seen: dict = defaultdict(set)
_fk_refs: dict = defaultdict(set)


def reset_tracking():
    _ids_seen.clear()
    _fk_refs.clear()


# ── Configuration validation ────────────────────────────────────────────────
def validate_configuration(config: dict):
    """Required keys for the Zoho Creator connector.

    `account_owner_name` (string) OR `workspaces` (list) must be provided —
    the workspace name(s) whose apps to sync. Everything else is mandatory.
    """
    missing = []
    for key in ("client_id", "client_secret", "refresh_token", "data_center"):
        if not config.get(key):
            missing.append(key)
    if not config.get("workspaces") and not config.get("account_owner_name"):
        missing.append("workspaces (list) or account_owner_name (string)")
    if missing:
        raise ValueError(
            f"Missing required configuration key(s): {', '.join(missing)}. "
            f"See configuration.json.example."
        )


# ── Record flattening ───────────────────────────────────────────────────────
_ADDRESS_SUBKEYS = {
    "address_line_1", "address_line_2", "district_city",
    "state_province", "postal_code", "country",
    "latitude", "longitude",
}

# Zoho's "Name" composite field always carries these sub-keys (some may be
# empty strings). Detecting any combination of these signals a Name dict.
_NAME_SUBKEYS = {"prefix", "first_name", "last_name", "suffix"}


def _is_address_dict(v: dict) -> bool:
    return any(k in _ADDRESS_SUBKEYS for k in v.keys())


def _is_name_dict(v: dict) -> bool:
    """Zoho Name composite: dict with no ID but at least one Name sub-key."""
    return ("ID" not in v) and any(k in v for k in _NAME_SUBKEYS)


def _is_file_dict(v: dict) -> bool:
    return ("ID" not in v) and any(k in v for k in ("filepath", "file_path", "url"))


def _flatten_name(prefix: str, v: dict, flat: dict) -> None:
    """Spread Name composite across `{prefix}_prefix`, `_first_name`,
    `_last_name`, `_suffix`, and `_display_value`. Empty strings become None."""
    for sub in ("prefix", "first_name", "last_name", "suffix"):
        val = v.get(sub)
        _merge_into_flat(
            flat,
            safe_column_name(f"{prefix}_{sub}"),
            val if val not in (None, "") else None,
        )
    dv = v.get("zc_display_value") or v.get("display_value")
    if dv:
        _merge_into_flat(flat, safe_column_name(f"{prefix}_display_value"), dv)


def _flatten_lookup(prefix: str, v: dict, flat: dict) -> None:
    _merge_into_flat(flat, safe_column_name(f"{prefix}_id"), v.get("ID"))
    _merge_into_flat(
        flat,
        safe_column_name(f"{prefix}_display_value"),
        v.get("zc_display_value") or v.get("display_value"),
    )


def _flatten_address(prefix: str, v: dict, flat: dict) -> None:
    for k, sub in v.items():
        col = safe_column_name(f"{prefix}_{k}")
        if isinstance(sub, (dict, list)):
            _merge_into_flat(flat, col, json.dumps(sub) if sub else None)
        else:
            _merge_into_flat(flat, col, sub)


def _flatten_file(prefix: str, v: dict, flat: dict) -> None:
    _merge_into_flat(
        flat,
        safe_column_name(f"{prefix}_url"),
        v.get("url") or v.get("file_path") or v.get("filepath"),
    )
    _merge_into_flat(
        flat,
        safe_column_name(f"{prefix}_filename"),
        v.get("file_name") or v.get("filename"),
    )


def _merge_into_flat(flat: dict, key: str, value):
    """Write `value` into `flat[key]`. If the key is already present (because
    two source keys collapsed to the same lowercase column name), merge by
    preferring a non-null/non-empty value over a null/empty one. If both
    are non-null and differ, keep the existing — the source order in a
    Python dict is stable, so this is "first-non-null wins"."""
    if key not in flat:
        flat[key] = value
        return
    existing = flat[key]
    if existing in (None, "") and value not in (None, ""):
        flat[key] = value


def flatten_record_auto(record: dict, drop_keys: set = None) -> dict:
    """Flatten a Zoho record into a flat dict suitable for op.upsert.

    Subforms (list-of-dicts) are NOT extracted here — pass their keys via
    `drop_keys` so the caller can write them to child tables separately.

    Column names are sanitised via `safe_column_name` (lowercased, dots
    collapsed). When multiple source keys collapse to the same target
    name (common for lookup fields where Bulk Read CSV exposes both
    `Field.id` and `Field_ID`), per-row collisions are merged by
    preferring non-null values."""
    drop_keys = drop_keys or set()
    flat: dict = {}

    for k, v in record.items():
        if k in drop_keys:
            continue
        safe_k = safe_column_name(k)
        if v is None or v == "":
            _merge_into_flat(flat, safe_k, None)
            continue
        if isinstance(v, dict):
            if not v:
                continue
            if "ID" in v:
                _flatten_lookup(k, v, flat)
                _fk_refs[k].add(v.get("ID"))
            elif _is_address_dict(v):
                _flatten_address(k, v, flat)
            elif _is_name_dict(v):
                _flatten_name(k, v, flat)
            elif _is_file_dict(v):
                _flatten_file(k, v, flat)
            else:
                _merge_into_flat(flat, safe_k, json.dumps(v))
        elif isinstance(v, list):
            # Either a multi-select (primitives), or a subform the caller
            # forgot to drop. JSON-encode either way.
            _merge_into_flat(flat, safe_k, json.dumps(v) if v else None)
        else:
            _merge_into_flat(flat, safe_k, v)

    return flat


# ── Upsert wrapper ──────────────────────────────────────────────────────────
def upsert(table: str, data: dict):
    try:
        op.upsert(table=table, data=data)
    except Exception as e:
        snippet = {k: f"{type(v).__name__}={repr(v)[:80]}" for k, v in data.items()}
        log.severe(f"Failed to upsert into {table}: {e}. Fields: {snippet}")
        raise

    id_val = data.get("ID") or data.get("id")
    if id_val is not None:
        _ids_seen[table].add(id_val)


def delete(table: str, keys: dict):
    op.delete(table=table, keys=keys)


# ── Hard-delete reconciliation ──────────────────────────────────────────────
def reconcile_deletes(table: str, current_ids: set, state: dict):
    """Diff current ID set against the previous full-sync snapshot in state.
    Anything in the previous set but not in the current set has been
    hard-deleted in Zoho → emit op.delete."""
    state_key = f"{table}__last_full_ids"
    prev = set(state.get(state_key) or [])
    deleted = prev - current_ids
    if deleted:
        log.info(f"{table}: {len(deleted)} hard-deleted record(s) → op.delete")
        for missing_id in deleted:
            try:
                delete(table, {"ID": missing_id})
            except Exception as e:
                log.warning(f"  delete({table}, ID={missing_id}) raised {e!r}")
    # JSON-serializable list, sorted for stable diffs.
    state[state_key] = sorted(str(i) for i in current_ids)


# ── Full-sync scheduling ────────────────────────────────────────────────────
def should_full_sync(state: dict, table: str) -> bool:
    last = state.get(f"{table}__last_full_sync")
    if not last:
        return True
    return (time.time() - float(last)) > FULL_SYNC_INTERVAL_SECONDS


def mark_full_sync(state: dict, table: str):
    state[f"{table}__last_full_sync"] = time.time()


# ── Orphan-recovery diagnostics ─────────────────────────────────────────────
def log_orphan_diagnostics():
    unresolved = {}
    for col, refs in _fk_refs.items():
        seen = _ids_seen.get(col, set())
        missing = refs - seen
        if missing:
            unresolved[col] = len(missing)

    if not unresolved:
        log.info("Orphan diagnostics: no unresolved cross-table references.")
        return
    log.warning(
        f"Orphan diagnostics: {len(unresolved)} lookup column(s) reference "
        f"IDs not present in their target table. Counts: {dict(unresolved)}."
    )
