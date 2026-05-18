"""
Shared utilities for the Breww connector — record flattening, child-table
extraction, upsert, and sync orchestration.

The flattening rules here are tuned for BI / warehouse use:
  - Foreign-key-shaped objects (anything with an `id` sub-field) are exploded
    into `<field>_id` + denormalized primitive columns so joins are trivial
    (`orders.customer_id = customers_suppliers.id`).
  - Address-shaped objects are flattened with a `_<subfield>` prefix.
  - Nested arrays that have no top-level endpoint but contain useful structured
    data (PO line items, BOM components, refund records) are extracted into
    their own child tables via CHILD_EXTRACTIONS.
  - Everything else is JSON-encoded for completeness, even if BI queries are
    awkward against it.
"""

import json

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

STATE_VERSION = "2"

# Address-shaped sub-objects: flatten with prefix instead of treating as FK.
_ADDRESS_FIELDS = {"billing_address", "delivery_address", "address"}

# Per-parent: nested arrays we extract into their own child tables.
# Each entry: array field name on parent → (child_table_name, parent_fk_column).
# The array field is then DROPPED from the parent row (data lives in the child).
CHILD_EXTRACTIONS = {
    "orders": [
        # /orders/ has its own /order-lines/ and /order-adjustment-lines/ endpoints
        # — those arrays are dropped (handled below in _DROP_ARRAYS_ON_PARENT).
        # payments_refunds has no top-level endpoint and carries refund-specific
        # fields (parent_customer_payment, method, etc.) not present on /payments/.
        ("payments_refunds", "order_payments_refunds", "order_id"),
    ],
    "products": [
        ("component_drinks", "product_component_drinks", "product_id"),
        ("component_stock_items", "product_component_stock_items", "product_id"),
    ],
    "purchase_orders": [
        # PO line items — stock_item, quantity, price per line. Critical for BI.
        ("entries", "purchase_order_entries", "purchase_order_id"),
    ],
}

# Per-parent: nested arrays we drop entirely (data already comes through a
# dedicated top-level endpoint, so re-ingesting would duplicate).
_DROP_ARRAYS_ON_PARENT = {
    "orders": {"order_lines", "adjustment_lines"},
}


def validate_configuration(configuration: dict):
    """Raise ValueError if required config keys are missing."""
    if not configuration.get("api_key"):
        raise ValueError("Missing required configuration key: api_key")


def _flatten_fk_object(prefix: str, obj: dict, flat: dict) -> None:
    """
    Explode an FK-shaped dict (one containing an `id` sub-field) into the
    parent flat dict using `<prefix>_<subkey>` column names.

    Behavior per sub-field:
      - scalar (str/int/float/bool/None) → `<prefix>_<sub>` = value
      - dict with `id` → recurse one level: only `<prefix>_<sub>_id` is kept
        (further denormalization would explode column counts; the full record
        is available via the FK target table).
      - dict without `id` → JSON-encoded under `<prefix>_<sub>`
      - list → JSON-encoded under `<prefix>_<sub>`
    """
    for k, v in obj.items():
        col = f"{prefix}_{k}"
        if v is None:
            flat[col] = None
        elif isinstance(v, dict):
            if "id" in v:
                flat[f"{col}_id"] = v.get("id")
            else:
                flat[col] = json.dumps(v) if v else None
        elif isinstance(v, list):
            flat[col] = json.dumps(v) if v else None
        else:
            flat[col] = v


def flatten_record(record: dict, table: str = None) -> dict:
    """
    Convert a Breww API record into a flat dict suitable for op.upsert.

    Per-field rules:
      - None / scalar → pass through.
      - Address-shaped dict (billing_address / delivery_address / address) →
        flatten with `<field>_<sub>` prefix.
      - Dict containing `id` (FK shape) → `<field>_id` + denormalized primitives
        via _flatten_fk_object.
      - Dict without `id` → JSON-encode as single column (e.g. custom_fields).
      - Arrays listed in CHILD_EXTRACTIONS[table] → dropped from the parent
        (they're handled by extract_children).
      - Arrays listed in _DROP_ARRAYS_ON_PARENT[table] → dropped (data lives
        in a separate top-level endpoint).
      - Any other array → JSON-encode as single column.
    """
    flat = {}
    child_arrays = {a for (a, _, _) in CHILD_EXTRACTIONS.get(table, [])}
    drop_arrays = _DROP_ARRAYS_ON_PARENT.get(table, set())

    for k, v in record.items():
        if k in child_arrays or k in drop_arrays:
            continue
        if v is None:
            flat[k] = None
        elif isinstance(v, dict):
            if k in _ADDRESS_FIELDS:
                # Address: flatten with prefix; sub-dicts/lists get JSON-encoded.
                for sub_k, sub_v in v.items():
                    if isinstance(sub_v, (dict, list)):
                        flat[f"{k}_{sub_k}"] = json.dumps(sub_v) if sub_v else None
                    else:
                        flat[f"{k}_{sub_k}"] = sub_v
            elif "id" in v:
                # FK-shaped object → explode into <k>_id + denormalized columns
                _flatten_fk_object(k, v, flat)
            else:
                # Generic dict without id (e.g. custom_fields) — JSON-encode.
                flat[k] = json.dumps(v)
        elif isinstance(v, list):
            flat[k] = json.dumps(v) if v else None
        else:
            flat[k] = v

    return flat


def extract_children(raw_record: dict, table: str) -> list:
    """
    For each nested array on `raw_record` that maps to a child table, return
    a list of (child_table, child_row) tuples ready for upsert. Each child row
    has been passed through flatten_record itself (so nested FK shapes inside
    a child element are also exploded).

    Returns [] when the parent table has no child extractions configured.
    """
    extractions = CHILD_EXTRACTIONS.get(table)
    if not extractions:
        return []

    parent_id = raw_record.get("id")
    rows = []
    for array_field, child_table, parent_fk in extractions:
        arr = raw_record.get(array_field) or []
        if not isinstance(arr, list):
            continue
        for elem in arr:
            if not isinstance(elem, dict):
                continue
            # Recursively flatten each child element so FK objects inside it
            # (e.g. purchase_order_entries[*].stock_item) also become <field>_id.
            child_row = flatten_record(elem, table=child_table)
            child_row[parent_fk] = parent_id
            rows.append((child_table, child_row))
    return rows


def upsert(table: str, data: dict):
    """Upsert wrapper with helpful error context on failure."""
    try:
        op.upsert(table=table, data=data)
    except Exception as e:
        snippet = {k: f"{type(v).__name__}={repr(v)[:80]}" for k, v in data.items()}
        log.severe(f"Failed to upsert into {table}: {e}. Fields: {snippet}")
        raise


def sync_table(config: dict, state: dict, *, table: str, endpoint: str,
               cursor_field: str = None, ordering: str = None,
               checkpoint_every: int = 100):
    """
    Generic per-resource sync.

    Iterates raw API records (not pre-flattened), extracts any configured
    child-table rows, then flattens the parent and upserts.
    """
    # Late import — api_client imports helpers for its own use, so we defer.
    from api_client import fetch_all_pages  # noqa: WPS433

    cursor_key = table
    last_cursor = state.get(cursor_key) if cursor_field else None

    params = {}
    if cursor_field:
        if ordering is None:
            ordering = cursor_field
        params["ordering"] = ordering
        if last_cursor:
            params[f"{cursor_field}__gte"] = last_cursor

    max_seen = last_cursor
    n = 0
    for raw in fetch_all_pages(config, endpoint, params=params):
        # Extract child rows first, BEFORE flatten_record drops the arrays.
        for child_table, child_row in extract_children(raw, table):
            upsert(child_table, child_row)

        flat = flatten_record(raw, table=table)
        upsert(table, flat)
        n += 1

        if cursor_field:
            val = raw.get(cursor_field)
            if val and (max_seen is None or val > max_seen):
                max_seen = val

        if n % checkpoint_every == 0:
            if cursor_field and max_seen and max_seen != last_cursor:
                state[cursor_key] = max_seen
            op.checkpoint(state)
            # Mid-resource checkpoints are frequent; keep at FINE so the
            # per-table INFO summary still stands out.
            log.fine(f"{table}: checkpointed at {n} records (cursor={max_seen})")

    if cursor_field and max_seen and max_seen != last_cursor:
        state[cursor_key] = max_seen
    op.checkpoint(state)
    log.info(f"{table}: synced {n} records (cursor={max_seen if cursor_field else 'full-sync'})")
