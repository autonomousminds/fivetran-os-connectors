"""
Shared utilities for the Zoho Bookings connector — config validation, record
flattening, upsert wrapper, hard-delete reconciliation.

Bookings responses are simpler than Creator's: no subforms, no Name/Address
composites. The flattening rules here mostly handle:
  - Nested dicts like `customer_more_info` (custom-fields object) → flattened
    with `{field}_{sub}` prefix.
  - Arrays (`assigned_staffs`, `assigned_services`, etc.) → JSON-encoded into
    one column. The caller's tables_meta layer emits its own bridge-table rows
    for these and passes the array keys via `drop_keys` so they don't end up
    inline.
  - Everything else → pass-through after column name sanitisation.
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
    back to 'col' to keep destination DDL valid."""
    cleaned = _COL_SAFE_RE.sub("_", str(name)).lower()
    return cleaned.strip("_") or "col"


# Per-table ID tracking, used by reconcile_deletes to detect hard deletes.
_ids_seen: dict = defaultdict(set)


def reset_tracking():
    _ids_seen.clear()


# ── Configuration validation ────────────────────────────────────────────────
def validate_configuration(config: dict):
    """Required keys for the Zoho Bookings connector.

    `workspaces` is optional — if absent or empty, we fall back to whatever
    workspaces the OAuth token can see (read via /workspaces). It's only
    truly required when the token spans multiple workspaces and the user
    wants to sync only a subset.
    """
    missing = []
    for key in ("client_id", "client_secret", "refresh_token", "data_center"):
        if not config.get(key):
            missing.append(key)
    if missing:
        raise ValueError(
            f"Missing required configuration key(s): {', '.join(missing)}. "
            f"See configuration.json.example."
        )


# ── Record flattening ───────────────────────────────────────────────────────
def _merge_into_flat(flat: dict, key: str, value):
    """Write `value` into `flat[key]`. If the key is already present (two
    source keys collapsed to the same lowercase column name), prefer a
    non-null/non-empty value over a null/empty one."""
    if key not in flat:
        flat[key] = value
        return
    existing = flat[key]
    if existing in (None, "") and value not in (None, ""):
        flat[key] = value


def flatten_record_auto(record: dict, drop_keys: set = None,
                        nested_prefix_keys: set = None) -> dict:
    """Flatten a Zoho Bookings record into a flat dict suitable for op.upsert.

    - Nested dicts whose key is in `nested_prefix_keys` (e.g. `customer_more_info`)
      get spread into `{key}_{subkey}` columns.
    - Any other nested dict is JSON-encoded into a single column.
    - Lists (multi-select arrays) are JSON-encoded.
    - Keys listed in `drop_keys` are skipped entirely (used to keep array
      fields out of the parent row when the caller emits bridge rows instead).
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
            if k in nested_prefix_keys:
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


# ── Upsert wrapper ──────────────────────────────────────────────────────────
def upsert(table: str, data: dict, id_key: str = "id"):
    """Wrap op.upsert with logging and ID tracking for hard-delete diffs.

    `id_key` lets callers track non-`id` primary keys (e.g. `booking_id`).
    Composite keys (bridge tables) are tracked as `(a, b)` tuples by passing
    `id_key` like 'service_id|staff_id'.
    """
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
    """Diff `current_ids` against the previous run's snapshot in state. Emit
    `op.delete` for anything in the previous set but not in the current set.

    `key_template` shapes the `op.delete` keys dict:
      - `None`           → `{"id": <value>}` (default single-PK)
      - `"<col>"` (str)  → `{<col>: <value>}` (single-PK, non-`id` column)
      - `{<col>: <idx>}` → composite-PK; `<idx>` is the index into the tuple
        stored in `current_ids`.
    """
    state_key = f"{table}__last_full_ids"
    prev_raw = state.get(state_key) or []
    # Stored as JSON-serialisable lists; rehydrate composites if needed.
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
