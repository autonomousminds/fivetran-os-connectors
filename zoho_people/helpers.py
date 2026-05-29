"""
Shared utilities for the Zoho People connector — config validation, record
flattening, upsert wrapper, hard-delete reconciliation.

## Flattening philosophy (Zoho People specifics)

Zoho People records arrive in one of two shapes:

1. **`/api/forms/{view}/records`** — flat dicts. Keys are display names
   from the form (e.g. `"First Name"`, `"Employee ID"`, `"Department"`).
   Lookup fields appear as dicts with `Zoho_ID` or appended `.ID` keys.
   We flatten to lower-snake-case columns and explode lookups.

2. **`/people/api/forms/{form}/getRecords`** — top-level dict keyed by
   recordId, each value carrying `tabularSections` arrays for nested
   sections like work experience, education, dependents.

3. **Module endpoints** (attendance, leave, timesheet, files) — bespoke
   shapes per module, handled by per-table modules directly.

For (1) we sanitise column names to snake_case alphanumeric, explode
lookup dicts into `{name}_id` + `{name}_display_value`, JSON-encode
unknown nested objects, and DROP list-of-dicts subform fields so the
caller can emit them as child-table rows.
"""

import json
import re
from collections import defaultdict

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

STATE_VERSION = "1"


_COL_SAFE_RE = re.compile(r"[^A-Za-z0-9_]+")


def safe_column_name(name: str) -> str:
    """Lower-case, alphanumeric/underscore only. Collapses dots, spaces,
    parens, and any other punctuation to underscores. Empty results fall
    back to 'col' to keep destination DDL valid.

    Zoho People uses HUMAN-READABLE field labels as JSON keys for the
    forms records endpoint (e.g. `"First Name"`, `"Date of Joining"`).
    These need aggressive sanitisation."""
    cleaned = _COL_SAFE_RE.sub("_", str(name)).lower()
    return cleaned.strip("_") or "col"


def safe_table_suffix(name: str) -> str:
    """Lower-snake-case suffix for dynamic child table names. Same rules
    as safe_column_name but with a different empty-fallback."""
    return _COL_SAFE_RE.sub("_", str(name).lower()).strip("_") or "sub"


# Per-table ID tracking, used by reconcile_deletes to detect hard deletes.
_ids_seen: dict = defaultdict(set)


def reset_tracking():
    _ids_seen.clear()


# ── Configuration validation ────────────────────────────────────────────────
def validate_configuration(config: dict):
    """Required keys for the Zoho People connector. Most values are strings
    (Fivetran requires it). Optional ints are parsed at the point of use."""
    missing = []
    for key in ("client_id", "client_secret", "refresh_token", "data_center"):
        if not config.get(key):
            missing.append(key)
    if missing:
        raise ValueError(
            f"Missing required configuration key(s): {', '.join(missing)}. "
            f"See configuration.json.example."
        )


def config_int(config: dict, key: str, default: int) -> int:
    val = config.get(key)
    if val in (None, ""):
        return default
    try:
        return int(str(val))
    except ValueError:
        log.warning(f"Configuration key {key}={val!r} is not an int — "
                    f"falling back to default {default}")
        return default


def config_bool(config: dict, key: str, default: bool = False) -> bool:
    val = config.get(key)
    if val in (None, ""):
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


# ── Record flattening ───────────────────────────────────────────────────────
def _merge_into_flat(flat: dict, key: str, value):
    """Write `value` into `flat[key]`. If the key is already present (because
    two source keys collapsed to the same lowercase column name), prefer a
    non-null/non-empty value over a null/empty one."""
    if key not in flat:
        flat[key] = value
        return
    existing = flat[key]
    if existing in (None, "") and value not in (None, ""):
        flat[key] = value


def _looks_like_lookup_dict(v: dict) -> bool:
    """Zoho People lookup fields appear as dicts with one of these shapes:
       {"id": "...", "displayValue": "..."}
       {"Zoho_ID": ..., "field": "..."}
       {"recordId": "..."}
    """
    if not isinstance(v, dict) or not v:
        return False
    keys = set(v.keys())
    if "id" in keys and ("displayValue" in keys or "value" in keys or "name" in keys):
        return True
    if "Zoho_ID" in keys:
        return True
    return False


def _flatten_lookup(prefix: str, v: dict, flat: dict) -> None:
    """Explode a lookup dict into `{prefix}_id` + `{prefix}_display_value`."""
    lookup_id = v.get("id") or v.get("Zoho_ID") or v.get("recordId")
    display = (v.get("displayValue") or v.get("display_value")
               or v.get("name") or v.get("value"))
    _merge_into_flat(flat, safe_column_name(f"{prefix}_id"), lookup_id)
    if display is not None:
        _merge_into_flat(flat, safe_column_name(f"{prefix}_display_value"), display)


def flatten_record_auto(record: dict, drop_keys: set = None,
                        nested_prefix_keys: set = None) -> dict:
    """Flatten a Zoho People record into a flat dict suitable for op.upsert.

    Rules:
      - Empty strings / None → None.
      - Dict that looks like a lookup → explode to `{key}_id` + `{key}_display_value`.
      - Dict listed in `nested_prefix_keys` → spread sub-keys as
        `{key}_{subkey}` columns.
      - Any other dict → JSON-encoded into one column.
      - List of dicts → JSON-encoded UNLESS the key is in `drop_keys`
        (caller is emitting a child table for it).
      - List of primitives → JSON-encoded into one column.
      - Everything else → pass-through after column name sanitisation.
    """
    drop_keys = drop_keys or set()
    nested_prefix_keys = nested_prefix_keys or set()
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
            if _looks_like_lookup_dict(v):
                _flatten_lookup(k, v, flat)
            elif k in nested_prefix_keys:
                for sub_k, sub_v in v.items():
                    sub_col = safe_column_name(f"{k}_{sub_k}")
                    if isinstance(sub_v, (dict, list)):
                        _merge_into_flat(flat, sub_col,
                                         json.dumps(sub_v) if sub_v else None)
                    else:
                        _merge_into_flat(flat, sub_col, sub_v)
            else:
                _merge_into_flat(flat, safe_k, json.dumps(v))
        elif isinstance(v, list):
            _merge_into_flat(flat, safe_k, json.dumps(v) if v else None)
        else:
            _merge_into_flat(flat, safe_k, v)

    return flat


def extract_subforms(record: dict) -> dict:
    """Pull every list-of-dicts field out of a record and return them as
    `{field_name: [list of dicts]}`. The caller drops these keys via the
    `drop_keys=` arg to `flatten_record_auto` and writes them to child
    tables."""
    out = {}
    for k, v in record.items():
        if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
            out[k] = v
    return out


# ── Upsert wrapper ──────────────────────────────────────────────────────────
def upsert(table: str, data: dict, id_key: str = "id"):
    """Wrap op.upsert with ID tracking for hard-delete reconciliation.

    `id_key` lets callers track non-`id` primary keys (e.g. `record_id`,
    `leave_id`). Composite keys are tracked as tuples by passing `id_key`
    like `'employee_id|date'`."""
    try:
        op.upsert(table=table, data=data)
    except Exception as e:
        snippet = {k: f"{type(v).__name__}={repr(v)[:80]}" for k, v in data.items()}
        log.severe(f"Failed to upsert into {table}: {e}. Fields: {snippet}")
        raise

    if "|" in id_key:
        parts = id_key.split("|")
        composite = tuple(data.get(p) for p in parts)
        if all(p is not None for p in composite):
            _ids_seen[table].add(composite)
    else:
        id_val = data.get(id_key)
        if id_val is not None:
            _ids_seen[table].add(id_val)


def delete(table: str, keys: dict):
    op.delete(table=table, keys=keys)


def ids_seen(table: str) -> set:
    return set(_ids_seen.get(table, set()))


# ── Hard-delete reconciliation ──────────────────────────────────────────────
def reconcile_deletes(table: str, current_ids: set, state: dict,
                      key_template=None):
    """Diff `current_ids` against the previous run's snapshot in state.
    Emit `op.delete` for anything in the previous set but not in the
    current set.

    `key_template` shapes the `op.delete` keys dict:
      - `None`           → `{"id": <value>}` (default single-PK)
      - `"<col>"` (str)  → `{<col>: <value>}` (single-PK, non-`id` column)
      - `{<col>: <idx>}` → composite-PK; `<idx>` is the index into the tuple
        stored in `current_ids`.

    Only fires for tables where the connector performs a FULL re-sync; for
    incremental tables we never see the full ID set in a single run, so
    reconciliation would emit spurious deletes."""
    state_key = f"{table}__last_full_ids"
    prev_raw = state.get(state_key) or []
    prev = set()
    for item in prev_raw:
        if isinstance(item, list):
            prev.add(tuple(item))
        else:
            prev.add(item)

    deleted = prev - current_ids
    if deleted:
        log.info(f"{table}: {len(deleted)} hard-deleted record(s) → op.delete")
        for missing in deleted:
            try:
                if isinstance(key_template, dict):
                    keys = {col: missing[idx] for col, idx in key_template.items()}
                elif isinstance(key_template, str):
                    keys = {key_template: missing}
                else:
                    keys = {"id": missing}
                delete(table, keys)
            except Exception as e:
                log.warning(f"  delete({table}, {missing!r}) raised {e!r}")

    serialisable = []
    for item in current_ids:
        if isinstance(item, tuple):
            serialisable.append(list(item))
        else:
            serialisable.append(str(item))
    serialisable.sort(key=lambda x: str(x))
    state[state_key] = serialisable
