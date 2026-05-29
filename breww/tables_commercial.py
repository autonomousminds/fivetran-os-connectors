"""Sync functions for Breww commercial / CRM resources (orders, customers, payments, CRM)."""

from fivetran_connector_sdk import Logging as log

from helpers import _ids_seen, sync_per_order, sync_table

# Tables that are FULL-RESYNC each run (no cursor support in the OpenAPI
# schema, OR cursor would miss edits — see /api/schema/ for the ground truth).
# `orders`, `order_lines`, `payments` get bespoke sync functions defined
# below (incremental + per-order-child strategy) so they're omitted here.
_FULL_SYNC_RESOURCES = [
    ("order_adjustment_lines",     "/order-adjustment-lines/"),
    ("customers_suppliers",        "/customers-suppliers/"),
    ("contacts",                   "/contacts/"),
    ("customer_types",             "/customer-types/"),
    ("customer_delivery_windows",  "/customer-delivery-windows/"),
    ("credit_notes",               "/credit-notes/"),
    ("credit_note_lines",          "/credit-note-lines/"),
    ("credit_note_allocations",    "/credit-note-allocations/"),
    ("customer_payments",          "/customer-payments/"),
    ("tax_rates",                  "/tax-rates/"),
    ("deals",                      "/deals/"),
    ("crm_activities",             "/crm-activities/"),
    ("crm_activity_types",         "/crm-activity-types/"),
]

# State keys used to coordinate the orders → children strategy switch.
# Set by sync_orders, consumed by sync_order_lines / sync_payments.
_STATE_KEY_STRATEGY            = "_orders_child_strategy"           # "full" | "per_order"
_STATE_KEY_INITIAL_FULL_DONE   = "_children_initial_full_sync_done" # bool
_STATE_KEY_FULL_DONE_THIS_RUN  = "_children_full_synced_this_run"   # list of table names
_PENDING_KEY_ORDER_LINES       = "_pending_per_order_order_lines"
_PENDING_KEY_PAYMENTS          = "_pending_per_order_payments"

# Tables whose per-order full-sync must complete before we flip
# _children_initial_full_sync_done = True.
_FULL_SYNC_GATE_TABLES = {"order_lines", "payments"}


def sync_orders(config, state):
    """
    Incremental sync of /orders/ on last_modified_at, plus strategy selection
    for the per-order children (order_lines, payments).

    Strategy:
      - First run after deploy / state reset, OR if the previous run's
        full-sync of children did NOT both complete → "full". All children
        do a full /endpoint/ sync.
      - Steady state (cursor set AND _children_initial_full_sync_done is
        True) → "per_order". Children fetch only the rows tied to orders
        modified this run, plus any leftover orders from a crashed previous
        per-order pass.
    """
    last_cursor = state.get("orders")
    children_done = state.get(_STATE_KEY_INITIAL_FULL_DONE, False)
    if last_cursor is None or not children_done:
        strategy = "full"
    else:
        strategy = "per_order"
    state[_STATE_KEY_STRATEGY] = strategy
    # Reset the per-run "which full-syncs completed" tracker.
    state[_STATE_KEY_FULL_DONE_THIS_RUN] = []

    log.info(f"orders → children strategy this run: {strategy}")
    sync_table(config, state, table="orders", endpoint="/orders/",
               cursor_field="last_modified_at")

    # After orders sync, if strategy is per_order, enqueue this run's modified
    # ids into each child's pending list (merged with any leftover from a
    # prior crash).
    if strategy == "per_order":
        new_ids = sorted(_ids_seen.get("orders", set()))
        for pkey in (_PENDING_KEY_ORDER_LINES, _PENDING_KEY_PAYMENTS):
            existing = set(state.get(pkey) or [])
            state[pkey] = sorted(existing | set(new_ids))


def _mark_full_done_and_maybe_flip(state, table):
    """Record that `table` completed its full-sync this run. When both
    gate tables have completed, flip _children_initial_full_sync_done so
    subsequent runs switch to per-order."""
    done = set(state.get(_STATE_KEY_FULL_DONE_THIS_RUN) or [])
    done.add(table)
    state[_STATE_KEY_FULL_DONE_THIS_RUN] = sorted(done)
    if _FULL_SYNC_GATE_TABLES.issubset(done):
        state[_STATE_KEY_INITIAL_FULL_DONE] = True
        log.info(
            "children initial full-sync done — order_lines + payments will "
            "switch to per-order strategy from the next run on"
        )


def sync_order_lines(config, state):
    """Full sync on initial backfill, per-order in steady state."""
    strategy = state.get(_STATE_KEY_STRATEGY, "full")
    if strategy == "full":
        sync_table(config, state, table="order_lines", endpoint="/order-lines/")
        _mark_full_done_and_maybe_flip(state, "order_lines")
    else:
        # Per-order: drains the pending list (merged with any leftover ids
        # from a previous crash). Each request returns lines for ONE order;
        # response is small (avg ~2 lines/order at this brewery).
        pending = state.get(_PENDING_KEY_ORDER_LINES) or []
        sync_per_order(
            config, state, table="order_lines", endpoint="/order-lines/",
            order_ids=pending, fk_param="order",
            pending_state_key=_PENDING_KEY_ORDER_LINES,
        )


def sync_payments(config, state):
    """Full sync on initial backfill, per-order in steady state."""
    strategy = state.get(_STATE_KEY_STRATEGY, "full")
    if strategy == "full":
        sync_table(config, state, table="payments", endpoint="/payments/")
        _mark_full_done_and_maybe_flip(state, "payments")
    else:
        pending = state.get(_PENDING_KEY_PAYMENTS) or []
        sync_per_order(
            config, state, table="payments", endpoint="/payments/",
            order_ids=pending, fk_param="order",
            pending_state_key=_PENDING_KEY_PAYMENTS,
        )


def _make_full_sync(table, endpoint):
    def _fn(config, state):
        sync_table(config, state, table=table, endpoint=endpoint, cursor_field=None)
    _fn.__name__ = f"sync_{table}"
    return _fn


_FULL_ENDPOINTS = dict(_FULL_SYNC_RESOURCES)


def _f(table):
    return _make_full_sync(table, _FULL_ENDPOINTS[table])


# Order matters: sync_orders MUST run before sync_order_lines /
# sync_payments — they read the strategy + pending lists it populates.
# Otherwise the listing matches the historical sync order.
COMMERCIAL_SYNCS = [
    sync_orders,
    sync_order_lines,
    _f("order_adjustment_lines"),
    _f("customers_suppliers"),
    _f("contacts"),
    _f("customer_types"),
    _f("customer_delivery_windows"),
    _f("credit_notes"),
    _f("credit_note_lines"),
    _f("credit_note_allocations"),
    _f("customer_payments"),
    sync_payments,
    _f("tax_rates"),
    _f("deals"),
    _f("crm_activities"),
    _f("crm_activity_types"),
]
