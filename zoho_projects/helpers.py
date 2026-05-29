"""
Shared utilities for the Zoho Projects connector — config validation, record
flattening, custom-field extraction, paired-date normalisation, upsert
wrapper, hard-delete reconciliation.

## Flattening philosophy (Zoho Projects specifics)

Zoho Projects records have four notable shapes that need handling beyond a
plain recursive JSON-to-columns flatten:

1. **Paired dates.** Almost every date field appears twice: a human-readable
   string in the portal's configured timezone (e.g. `created_time = "01-31-2024
   14:23:05"`) and a UTC epoch-milliseconds companion (`created_time_long =
   1706711085000`). We ALWAYS consume the `*_long` field and additionally
   stamp a normalised ISO-8601 `*_at` column on the record. The string
   counterpart is kept as-is (named `*_display` would diverge from the
   source field name; we don't rename it).

2. **Custom fields (UDFs).** Custom fields appear as top-level keys with
   the stable shape `UDF_<TYPE><N>` — e.g. `UDF_CHAR1`, `UDF_DATE1`,
   `UDF_NUMBER1`, `UDF_MULTI1`, `UDF_MULTIUSER1`. These are STRIPPED from
   the parent row by `extract_udfs` and emitted into the corresponding
   `*_custom_fields` child table by the caller, indexed by `parent_id` +
   `field_api_name`. Stable when the portal admin renames or adds fields.

3. **Lookup dicts.** Nested objects like `{"id": "...", "name": "..."}`
   for assignees, owners, etc. are exploded to `{field}_id` + `{field}_name`.

4. **Subforms / nested arrays.** List-of-dicts fields (e.g. `dependency`
   on Task Details) are NOT JSON-encoded — the caller pulls them out via
   `extract_subforms` and emits them to child tables.
"""

import json
import re
from collections import defaultdict
from datetime import datetime, timezone

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
    """Required keys for the Zoho Projects connector. Most values are strings
    (Fivetran requires it); lists/bools are interpreted at the point of use."""
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


def config_list(config: dict, key: str) -> list:
    """Coerce a config value to a list of strings. Accepts a real list or a
    JSON-encoded string list. Returns [] when missing/empty."""
    val = config.get(key)
    if val in (None, "", []):
        return []
    if isinstance(val, list):
        return [str(x) for x in val if x not in (None, "")]
    if isinstance(val, str):
        s = val.strip()
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(x) for x in parsed if x not in (None, "")]
            except json.JSONDecodeError:
                pass
        # Treat a bare string as a single-element list.
        return [s] if s else []
    return [str(val)]


# ── Date helpers ────────────────────────────────────────────────────────────
def ms_to_iso(ms) -> str:
    """Convert epoch milliseconds (int or stringified int) to UTC ISO-8601.
    Returns None on empty/invalid input."""
    if ms in (None, "", 0, "0"):
        return None
    try:
        ms_int = int(ms)
    except (TypeError, ValueError):
        return None
    try:
        dt = datetime.fromtimestamp(ms_int / 1000.0, tz=timezone.utc)
        return dt.isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def extract_paired_dates(flat: dict) -> dict:
    """For every `<name>_long` key in `flat`, stamp an ISO-8601 `<name>_at`
    column alongside it. Mutates `flat` in place and also returns it."""
    long_keys = [k for k in list(flat.keys()) if k.endswith("_long")]
    for lk in long_keys:
        base = lk[:-5]  # strip "_long"
        at_key = f"{base}_at"
        if at_key in flat:
            continue
        iso = ms_to_iso(flat.get(lk))
        if iso is not None:
            flat[at_key] = iso
    return flat


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
    """Zoho Projects nested-reference dicts. Common shapes:
       {"id": "...", "name": "..."}             — assignee/owner/creator
       {"id": "...", "first_name": "...", "last_name": "..."}
       {"id_string": "...", "name": "..."}      — V3 string-ID variant
       {"Zoho_ID": ..., "name": "..."}
    """
    if not isinstance(v, dict) or not v:
        return False
    keys = set(v.keys())
    has_id = bool(keys & {"id", "id_string", "Zoho_ID", "ID"})
    has_name = bool(keys & {"name", "first_name", "full_name", "displayName",
                            "display_name", "value"})
    return has_id and has_name


def _flatten_lookup(prefix: str, v: dict, flat: dict) -> None:
    """Explode a lookup dict into `{prefix}_id` + `{prefix}_name` (+ optional
    `{prefix}_email` when present — used heavily on user/assignee dicts)."""
    lookup_id = (v.get("id") or v.get("id_string") or v.get("Zoho_ID")
                 or v.get("ID"))
    name = (v.get("name") or v.get("full_name") or v.get("displayName")
            or v.get("display_name") or v.get("value"))
    if not name:
        fn = v.get("first_name") or ""
        ln = v.get("last_name") or ""
        joined = f"{fn} {ln}".strip()
        if joined:
            name = joined
    email = v.get("email") or v.get("email_id")
    _merge_into_flat(flat, safe_column_name(f"{prefix}_id"), lookup_id)
    if name is not None:
        _merge_into_flat(flat, safe_column_name(f"{prefix}_name"), name)
    if email is not None:
        _merge_into_flat(flat, safe_column_name(f"{prefix}_email"), email)


_UDF_RE = re.compile(r"^UDF_[A-Z]+\d+$")


def is_udf_key(k: str) -> bool:
    """Stable Zoho Projects custom-field identifier shape, e.g. UDF_CHAR1,
    UDF_DATE2, UDF_MULTIUSER3."""
    return bool(_UDF_RE.match(str(k or "")))


def flatten_record(record: dict, drop_keys: set = None,
                   nested_prefix_keys: set = None,
                   strip_udfs: bool = True) -> dict:
    """Flatten a Zoho Projects record into a flat dict suitable for op.upsert.

    Rules:
      - Empty strings / None → None.
      - UDF_<TYPE><N> keys → stripped from output when strip_udfs=True
        (caller extracts to child table via extract_udfs).
      - Dict that looks like a lookup → explode to `{key}_id` + `{key}_name`
        (+ `_email` when present).
      - Dict listed in `nested_prefix_keys` → spread sub-keys as
        `{key}_{subkey}` columns.
      - Any other dict → JSON-encoded into one column.
      - List of dicts → JSON-encoded UNLESS the key is in `drop_keys`
        (caller is emitting a child table for it).
      - List of primitives → JSON-encoded.
      - Everything else → pass-through after column name sanitisation.

    After flattening, every `*_long` epoch-ms column also receives a
    paired `*_at` ISO-8601 column (UTC).
    """
    drop_keys = drop_keys or set()
    nested_prefix_keys = nested_prefix_keys or set()
    flat: dict = {}

    for k, v in record.items():
        if k in drop_keys:
            continue
        if strip_udfs and is_udf_key(k):
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

    extract_paired_dates(flat)
    return flat


def extract_udfs(record: dict) -> list:
    """Pull every custom-field value out of `record`. Zoho Projects ships
    custom fields in two parallel shapes on the same response:

      1. Top-level keys with the stable `UDF_<TYPE><N>` API name (legacy V2).
      2. A `customfields` array of `{column_name, label_name, value}` dicts
         (V3 and some V2 endpoints — bugs notably use this shape).

    We walk both and return a list of `(field_api_name, raw_value)` tuples.
    Caller resolves `field_label` / `field_type` from the metadata table.
    """
    out = []
    for k, v in record.items():
        if is_udf_key(k):
            out.append((k, v))
    cf_array = record.get("customfields") if isinstance(record, dict) else None
    if isinstance(cf_array, list):
        for entry in cf_array:
            if not isinstance(entry, dict):
                continue
            api_name = (entry.get("column_name") or entry.get("api_name")
                        or entry.get("field_api_name"))
            if not api_name:
                continue
            out.append((str(api_name), entry.get("value")))
    return out


def extract_subforms(record: dict) -> dict:
    """Pull every list-of-dicts field out of a record and return them as
    `{field_name: [list of dicts]}`. The caller drops these keys via the
    `drop_keys=` arg to `flatten_record` and writes them to child tables."""
    out = {}
    for k, v in record.items():
        if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
            out[k] = v
    return out


# ── UDF child-row builder ──────────────────────────────────────────────────
def _coerce_udf_value(api_name: str, raw):
    """Bucket a UDF value into typed columns based on the API name's type
    code (CHAR/PICK/PICKLIST → text, NUMBER/LONG → numeric, DATE/DATETIME →
    date/datetime, BOOL → boolean). Multi-value variants are JSON-encoded
    into value_text.

    Returns a dict of `{value_text, value_number, value_date, value_datetime,
    value_boolean}` with exactly the relevant slot populated."""
    out = {"value_text": None, "value_number": None, "value_date": None,
           "value_datetime": None, "value_boolean": None}
    if raw is None or raw == "":
        return out

    # Type code = the alpha component of `UDF_<TYPE><N>`.
    m = re.match(r"^UDF_([A-Z]+)\d+$", str(api_name or ""))
    type_code = m.group(1) if m else ""

    if isinstance(raw, (list, dict)):
        # Multi-value or complex — JSON-encode into text.
        out["value_text"] = json.dumps(raw)
        return out

    if type_code in {"NUMBER", "LONG", "INT", "DECIMAL", "PERCENT", "CURRENCY"}:
        try:
            out["value_number"] = float(raw)
        except (TypeError, ValueError):
            out["value_text"] = str(raw)
        return out

    if type_code == "BOOL":
        out["value_boolean"] = str(raw).strip().lower() in ("1", "true", "yes", "on")
        return out

    if type_code == "DATE":
        out["value_date"] = str(raw)
        return out

    if type_code in {"DATETIME", "TIMESTAMP"}:
        # Could be epoch-ms or formatted string — keep both representations
        # via the timestamp slot; downstream consumers cast as needed.
        out["value_datetime"] = str(raw)
        return out

    # Default: text bucket.
    out["value_text"] = str(raw)
    return out


def build_udf_row(parent_keys: dict, api_name: str, raw_value,
                  meta_by_api_name: dict = None) -> dict:
    """Compose a single child-table row for a UDF value.

    `parent_keys` is the dict that uniquely identifies the parent (e.g.
    `{"portal_id": "...", "task_id": "..."}`). `meta_by_api_name` is an
    optional map keyed by `field_api_name` whose values carry
    `field_label` / `field_type` from the metadata discovery — when present
    we hydrate the row with the human label and the source-side type.
    """
    typed = _coerce_udf_value(api_name, raw_value)
    meta = (meta_by_api_name or {}).get(api_name) or {}
    row = dict(parent_keys)
    row.update({
        "field_api_name":  api_name,
        "field_label":     meta.get("field_label") or meta.get("label_name")
                           or meta.get("label"),
        "field_type":      meta.get("field_type") or meta.get("type")
                           or _udf_type_from_name(api_name),
        "raw_value":       raw_value if isinstance(raw_value, (str, int, float, bool))
                           else json.dumps(raw_value) if raw_value is not None else None,
    })
    row.update(typed)
    return row


def _udf_type_from_name(api_name: str) -> str:
    m = re.match(r"^UDF_([A-Z]+)\d+$", str(api_name or ""))
    return m.group(1).lower() if m else None


# ── Upsert wrapper ──────────────────────────────────────────────────────────
def upsert(table: str, data: dict, id_key: str = "id"):
    """Wrap op.upsert with ID tracking for hard-delete reconciliation.

    `id_key` lets callers track non-`id` primary keys (e.g. `task_id`,
    `bug_id`). Composite keys are tracked as tuples by passing `id_key`
    like `'portal_id|task_id'`."""
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
                      key_template=None, state_key_suffix: str = ""):
    """Diff `current_ids` against the previous run's snapshot in state.
    Emit `op.delete` for anything in the previous set but not in the
    current set.

    `key_template` shapes the `op.delete` keys dict:
      - `None`           → `{"id": <value>}` (default single-PK)
      - `"<col>"` (str)  → `{<col>: <value>}` (single-PK, non-`id` column)
      - `{<col>: <idx>}` → composite-PK; `<idx>` is the index into the tuple
        stored in `current_ids`.

    `state_key_suffix` lets callers scope the snapshot key by portal
    (so per-portal full-syncs reconcile independently). Pass e.g.
    `state_key_suffix=f"__{portal_id}"`.
    """
    state_key = f"{table}__last_full_ids{state_key_suffix}"
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
