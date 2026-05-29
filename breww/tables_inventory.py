"""Sync functions for Breww inventory / supply resources (products, stock, POs, suppliers)."""

from helpers import sync_table

# All inventory / supply tables are full-resynced on every run.
#   - inventory_receipts / purchase_orders / supplier_invoices have
#     `created_at` cursors per the OpenAPI schema, but they are lifecycle
#     entities (PO revisions, invoice corrections, quantity adjustments)
#     so a created-only cursor would miss edits.
#   - products / stock_items / container_types are reference data, edited
#     occasionally — full resync is cheap (<5 requests each).
#   - stock_received's `created_at` filter is NOT registered in the schema
#     (verified by curl: ?created_at__gte=2030-01-01 still returns 4 614 rows).
#     Full resync — Breww would need to register the filter for it to help.
#   - fulfillments lacks any date or FK filter — full resync only.
_RESOURCES = [
    ("products",                "/products/",                None),
    ("stock_items",             "/stock-items/",             None),
    ("stock_received",          "/stock-received/",          None),
    ("inventory_receipts",      "/inventory-receipts/",      None),
    ("purchase_orders",         "/purchase-orders/",         None),
    ("supplier_invoices",       "/supplier-invoices/",       None),
    ("container_types",         "/container-types/",         None),
    ("nr_container_brands",     "/nr-container-brands/",     None),
    ("goods_in_document_pools", "/goods-in-document-pools/", None),
    ("fulfillments",            "/fulfillments/",            None),
]


def _make_sync(table, endpoint, cursor_field):
    def _fn(config, state):
        sync_table(config, state, table=table, endpoint=endpoint,
                   cursor_field=cursor_field)
    _fn.__name__ = f"sync_{table}"
    return _fn


INVENTORY_SYNCS = [_make_sync(t, e, c) for (t, e, c) in _RESOURCES]
