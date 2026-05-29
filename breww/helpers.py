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
from collections import defaultdict

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

STATE_VERSION = "4"  # bumped: reset orders cursor after partial-sync data loss (see sync_table)

# ── Orphan-recovery tracking ────────────────────────────────────────────────
# Breww's list endpoints silently filter out soft-deleted records (e.g. archived
# customers, ex-employee users), but the detail endpoints `/<resource>/{id}/`
# still serve them with a `deleted` timestamp. We track every FK id referenced
# vs every parent id we synced, then fetch the difference at the end.
#
# `recover_orphans()` reads these dicts; they're populated by `upsert()` and
# `flatten_record()`. `reset_tracking()` clears them at the start of each run.
_ids_seen: dict = defaultdict(set)
_fk_refs: dict = defaultdict(set)

# Column-name-AFTER-flatten → (recovery_target_table, detail_endpoint_template).
# Whenever `upsert` writes a row containing one of these columns with a non-null
# value, the value is registered as a FK reference to the target table. The
# orphan recovery pass diffs (refs registered) − (ids upserted into target) and
# fetches each missing id from its detail endpoint.
#
# Keying on the flat column name (not the original Breww field name) lets the
# same logic catch BOTH FK shapes:
#   - nested FK objects like `customer: {"id": 4221, "name": "Acme", ...}`
#     which flatten_record explodes into `customer_id`, `customer_name`, ...
#   - scalar FK columns like `container_type_id: 4476` which appear directly
#     on child records (e.g. product_component_drinks elements have
#     `container_type_id` as a plain integer, with no nested object).
#
# A full-database FK audit found 4 target tables with orphan references; all
# 4 are recoverable via their detail endpoint:
#   - customers_suppliers: includes soft-deleted (carries `deleted` timestamp)
#   - users:               includes ex-employees
#   - stock_items:         includes items hidden by an undocumented list filter
#                          (orphans have obsolete=False, deleted=None — they
#                          look perfectly active, but `/stock-items/` omits
#                          them regardless of filter combination)
#   - container_types:     same hidden-filter pattern; orphans include
#                          fundamental formats like "440ml Can", "9G Firkin"
_FK_RECOVERY_BY_COLUMN = {
    # → customers_suppliers
    "customer_id":       ("customers_suppliers", "/customers-suppliers/{id}/"),
    "parent_company_id": ("customers_suppliers", "/customers-suppliers/{id}/"),
    # → users
    "created_by_id":     ("users",          "/users/{id}/"),
    "sales_person_id":   ("users",          "/users/{id}/"),
    "updater_id":        ("users",          "/users/{id}/"),
    "approver_id":       ("users",          "/users/{id}/"),
    "rejecter_id":       ("users",          "/users/{id}/"),
    "canceler_id":       ("users",          "/users/{id}/"),
    "deleter_id":        ("users",          "/users/{id}/"),
    "completed_by_id":   ("users",          "/users/{id}/"),
    # → stock_items
    "stock_item_id":     ("stock_items",    "/stock-items/{id}/"),
    # → container_types
    "container_type_id": ("container_types", "/container-types/{id}/"),
}


def reset_tracking():
    """Clear orphan-recovery tracking. Call once at the start of every sync run."""
    _ids_seen.clear()
    _fk_refs.clear()

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

# Per-parent: nested fields we drop entirely. Data is already available either
# via a dedicated top-level endpoint, or via a top-level FK column that
# flatten_record extracts from the same parent record.
_DROP_FIELDS_ON_PARENT = {
    # order_lines + adjustment_lines have own endpoints (/order-lines/, etc.)
    "orders": {"order_lines", "adjustment_lines"},
    # customers_suppliers.contacts is a denormalised array of the same records
    # as /contacts/ (verified: 1,588 contacts total, 432 customers carry a copy
    # of theirs in this nested array). Dropping saves ~140KB and avoids drift.
    "customers_suppliers": {"contacts"},
    # customer_payments.order_allocations is the same M:M data as the top-level
    # /payments/ table — each /payments/ row is one (customer_payment, order)
    # allocation. Verified: customer_payments=31,827, payments=30,836 (~1
    # allocation per customer_payment on average).
    "customer_payments": {"order_allocations"},
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
    drop_fields = _DROP_FIELDS_ON_PARENT.get(table, set())

    for k, v in record.items():
        if k in child_arrays or k in drop_fields:
            continue
        if v is None:
            flat[k] = None
        elif isinstance(v, dict):
            if not v:
                # Empty dict — emit nothing. Avoids cluttering the warehouse
                # with `column = '{}'` placeholders that mean "no value here"
                # (e.g. customers_suppliers.delivery_windows is always `{}`
                # when the customer has no delivery-window configuration).
                continue
            if k in _ADDRESS_FIELDS:
                # Address: flatten with prefix; sub-dicts/lists get JSON-encoded.
                for sub_k, sub_v in v.items():
                    if isinstance(sub_v, (dict, list)):
                        flat[f"{k}_{sub_k}"] = json.dumps(sub_v) if sub_v else None
                    else:
                        flat[f"{k}_{sub_k}"] = sub_v
            elif "id" in v:
                # FK-shaped object → explode into <k>_id + denormalized columns.
                # Orphan-recovery registration is handled centrally in `upsert`
                # via the resulting `<k>_id` column name (see _FK_RECOVERY_BY_COLUMN).
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
    """Upsert wrapper with helpful error context on failure.

    Also drives orphan-recovery tracking:
      - Records the row's `id` so we know we've successfully written it.
      - Scans the row's columns for any name in `_FK_RECOVERY_BY_COLUMN` and
        registers the value as a FK reference to that target table.
    """
    try:
        op.upsert(table=table, data=data)
    except Exception as e:
        snippet = {k: f"{type(v).__name__}={repr(v)[:80]}" for k, v in data.items()}
        log.severe(f"Failed to upsert into {table}: {e}. Fields: {snippet}")
        raise

    # Track the id we wrote. Child tables with composite PKs won't have a
    # top-level `id` field, and that's fine — only the recovery-target tables
    # (customers_suppliers, users, stock_items, container_types) ever need it.
    id_val = data.get("id")
    if id_val is not None:
        _ids_seen[table].add(id_val)

    # Register every FK reference this row makes, regardless of whether it
    # came from a nested object (flattened by _flatten_fk_object) or was a
    # scalar column on the source record.
    for col, val in data.items():
        if val is None:
            continue
        target = _FK_RECOVERY_BY_COLUMN.get(col)
        if target is not None:
            _fk_refs[target[0]].add(val)


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
    # If we entered with no cursor on a cursor-bearing table, this is a
    # full historical sync (first run after deploy / state reset). DO NOT
    # advance the cursor mid-table — if we crash on a rate limit before the
    # table completes, the soft-exit in connector.py would persist a
    # partial cursor and the unsynced tail would be invisible to every
    # subsequent run. Keep cursor=None until the loop completes so a retry
    # restarts cleanly from scratch (upserts are idempotent).
    initial_full_sync = cursor_field is not None and last_cursor is None

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
            if cursor_field and max_seen and max_seen != last_cursor and not initial_full_sync:
                state[cursor_key] = max_seen
            op.checkpoint(state)
            # Mid-resource checkpoints are frequent; keep at FINE so the
            # per-table INFO summary still stands out.
            log.fine(f"{table}: checkpointed at {n} records (cursor={max_seen}, initial_full_sync={initial_full_sync})")

    if cursor_field and max_seen and max_seen != last_cursor:
        state[cursor_key] = max_seen
    op.checkpoint(state)
    log.info(f"{table}: synced {n} records (cursor={max_seen if cursor_field else 'full-sync'})")


def sync_per_order(config: dict, state: dict, *, table: str, endpoint: str,
                   order_ids: list, fk_param: str = "order",
                   pending_state_key: str = None):
    """
    Per-parent sync: for each id in `order_ids`, GET `<endpoint>?<fk_param>=<id>`
    and upsert every record returned. This is the bulk-fetch substitute for
    children of `/orders/` — Breww's list endpoints silently ignore
    `?<fk>__in=` filters AND don't accumulate repeated `?<fk>=A&<fk>=B`
    (DRF default = "last value wins"), so one request per parent is
    the cheapest way to fetch only-the-changed children.

    Crash safety: the still-unprocessed ids are kept in
    `state[pending_state_key]`. On a rate-limit soft-exit mid-pass, the
    next run resumes with the surviving ids (plus any newly-modified ones
    added by the next orders sync). If `pending_state_key` is None, the
    function runs without persistence (caller must handle resume itself).
    """
    from api_client import fetch_all_pages  # noqa: WPS433

    # Normalise to a list of unique ids in stable order.
    pending = sorted(set(order_ids))
    if pending_state_key is not None:
        # Merge with anything left over from a prior crash.
        prior = state.get(pending_state_key) or []
        pending = sorted(set(pending) | set(prior))
        state[pending_state_key] = pending

    if not pending:
        log.info(f"{table}: per-order sync — no parents to process this run")
        return

    log.info(f"{table}: per-order sync starting — {len(pending)} parent orders")
    n = 0
    processed = 0
    while pending:
        oid = pending[0]
        try:
            for raw in fetch_all_pages(config, endpoint, params={fk_param: oid}):
                for child_table, child_row in extract_children(raw, table):
                    upsert(child_table, child_row)
                flat = flatten_record(raw, table=table)
                upsert(table, flat)
                n += 1
        except Exception:
            # Surface the failure to the caller. State still has `pending`
            # intact (this id NOT removed yet), so the next run resumes here.
            if pending_state_key is not None:
                state[pending_state_key] = pending
                op.checkpoint(state)
            raise
        # Success for this parent — remove from pending and persist.
        pending.pop(0)
        if pending_state_key is not None:
            state[pending_state_key] = pending
        processed += 1
        if processed % 50 == 0:
            op.checkpoint(state)
            log.fine(f"{table}: {processed} parents done, {len(pending)} remaining (records={n})")

    if pending_state_key is not None:
        state.pop(pending_state_key, None)
    op.checkpoint(state)
    log.info(f"{table}: per-order sync complete — {n} records across {processed} parents")


def recover_orphans(config: dict, state: dict, max_iterations: int = 3):
    """
    Post-sync pass: fetch records referenced by FK that were missing from the
    list endpoint. The list endpoints filter out (a) soft-deleted records and
    (b) records hidden by undocumented per-tenant filters; all such records
    are still reachable via their detail endpoint.

    For each target table in _FK_RECOVERY_BY_COLUMN's values, compute the set
    difference
        (refs registered during this run)
        − (ids successfully upserted this run)
        − (ids already resolved on any previous run, from state)
    then GET each remaining missing id from its detail endpoint and upsert.

    State-persistence is the key cost optimisation: once we've resolved an
    orphan id (either via successful upsert or via a 404), we remember it in
    `state["recovered_orphans"][target_table]` so we never re-fetch it. The
    list endpoint will still filter the row out next run, the same ref will
    show up in `_fk_refs` again, but we'll subtract it via `already_done` and
    skip the detail GET. Steady-state cost: near zero. Without this, every
    run pays the full initial-recovery cost (~1.7k detail GETs).

    Loops up to `max_iterations` times: a recovered record may itself reference
    FKs to other deleted records (e.g. a deleted customer's `created_by_id`
    points to an ex-employee user). Each iteration only fetches NEW orphans
    discovered since the last iteration, so the cost stays bounded.
    """
    from api_client import api_request, BASE_URL  # avoid circular import

    # Unique recovery targets, derived from the column registry.
    targets = {}
    for target_table, endpoint_template in _FK_RECOVERY_BY_COLUMN.values():
        targets[target_table] = endpoint_template

    # State is JSON, so persisted ids live as a list per target table. Load
    # into a set per table for fast membership/difference; write back as list
    # after each successful detail GET (so even a mid-pass crash retains
    # progress).
    state.setdefault("recovered_orphans", {})
    done_by_table = {
        t: set(state["recovered_orphans"].get(t, []))
        for t in targets
    }

    def _persist_done(target_table: str, fk_id):
        done_by_table[target_table].add(fk_id)
        state["recovered_orphans"][target_table] = sorted(done_by_table[target_table])

    grand_total_recovered = 0
    for iteration in range(1, max_iterations + 1):
        any_recovered_this_iter = False
        for target_table, endpoint_template in targets.items():
            refs = _fk_refs.get(target_table, set())
            seen = _ids_seen.get(target_table, set())
            already_done = done_by_table[target_table]
            missing = refs - seen - already_done
            if not missing:
                continue

            log.info(
                f"orphan-recovery [iter {iteration}] {target_table}: fetching "
                f"{len(missing)} records missing from list endpoint "
                f"({len(already_done)} previously resolved, skipped)"
            )
            recovered = 0
            not_found = 0
            for fk_id in sorted(missing):
                url = f"{BASE_URL}{endpoint_template.format(id=fk_id)}"
                data = api_request(config, url)
                if not data:
                    not_found += 1
                    # 404 — record genuinely gone. Persist so we never
                    # re-attempt on future runs.
                    _ids_seen[target_table].add(fk_id)
                    _persist_done(target_table, fk_id)
                    log.fine(f"  {target_table}/{fk_id}: 404 — skipping permanently")
                    continue
                flat = flatten_record(data, table=target_table)
                upsert(target_table, flat)  # registers any new FK refs from this record
                _persist_done(target_table, fk_id)
                recovered += 1

                if recovered % 50 == 0:
                    op.checkpoint(state)
                    log.fine(f"  {target_table}: recovered {recovered}/{len(missing)}")

            op.checkpoint(state)
            log.info(
                f"orphan-recovery [iter {iteration}] {target_table}: "
                f"recovered {recovered} ({not_found} returned 404)"
            )
            grand_total_recovered += recovered
            if recovered:
                any_recovered_this_iter = True

        if not any_recovered_this_iter:
            log.info(f"orphan-recovery: converged after {iteration} iteration(s)")
            break
    else:
        log.warning(
            f"orphan-recovery: hit max_iterations={max_iterations} without "
            f"converging — some second-order orphans may remain"
        )

    total_persisted = sum(len(v) for v in done_by_table.values())
    log.info(
        f"orphan-recovery: pass complete — {grand_total_recovered} new records "
        f"recovered this run across {len(targets)} tables; "
        f"{total_persisted} ids persisted total (cumulative across runs)"
    )
