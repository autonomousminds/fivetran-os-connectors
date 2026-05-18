"""Sync functions for Breww commercial / CRM resources (orders, customers, payments, CRM)."""

from helpers import sync_table

# (table_name, endpoint, cursor_field_or_None)
# Cursor field choice per resource is dictated by the Breww OpenAPI schema:
#   - orders is the only resource exposing `last_modified_at__gte` (true updated-since)
#   - customers-suppliers, credit-notes, crm-activities expose a created-at filter
#   - the remaining tables have no date filter and must be fully re-synced
_RESOURCES = [
    ("orders",                     "/orders/",                    "last_modified_at"),
    ("order_lines",                "/order-lines/",               None),
    ("order_adjustment_lines",     "/order-adjustment-lines/",    None),
    ("customers_suppliers",        "/customers-suppliers/",       "created_at"),
    ("contacts",                   "/contacts/",                  None),
    ("customer_types",             "/customer-types/",            None),
    ("customer_delivery_windows",  "/customer-delivery-windows/", None),
    ("credit_notes",               "/credit-notes/",              "created_on"),
    ("credit_note_lines",          "/credit-note-lines/",         None),
    ("credit_note_allocations",    "/credit-note-allocations/",   None),
    ("customer_payments",          "/customer-payments/",         None),
    ("payments",                   "/payments/",                  None),
    ("tax_rates",                  "/tax-rates/",                 None),
    ("deals",                      "/deals/",                     None),
    ("crm_activities",             "/crm-activities/",            "created_at"),
    ("crm_activity_types",         "/crm-activity-types/",        None),
]


def _make_sync(table, endpoint, cursor_field):
    def _fn(config, state):
        sync_table(config, state, table=table, endpoint=endpoint,
                   cursor_field=cursor_field)
    _fn.__name__ = f"sync_{table}"
    return _fn


COMMERCIAL_SYNCS = [_make_sync(t, e, c) for (t, e, c) in _RESOURCES]
