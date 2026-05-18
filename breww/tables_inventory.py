"""Sync functions for Breww inventory / supply resources (products, stock, POs, suppliers)."""

from helpers import sync_table

_RESOURCES = [
    ("products",                "/products/",                None),
    ("stock_items",             "/stock-items/",             None),
    ("stock_received",          "/stock-received/",          None),
    ("inventory_receipts",      "/inventory-receipts/",      "created_at"),
    ("purchase_orders",         "/purchase-orders/",         "created_at"),
    ("supplier_invoices",       "/supplier-invoices/",       "created_at"),
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
