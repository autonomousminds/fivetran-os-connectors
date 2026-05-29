"""Sync functions for Breww production resources (drinks, batches, vessels, fermentation)."""

from helpers import sync_table

# Cursor strategy per table — driven by Breww's OpenAPI schema at
# /api/schema/ (the authoritative list of which filters exist):
#   APPEND-ONLY  →  created_at / created_on cursor is safe (no edits to miss):
#     - drink_batch_actions, drink_batch_stock_items_used,
#       ingredient_batch_actions are write-once log/usage rows.
#   LIFECYCLE  →  no cursor (records get edited after creation):
#     - drink_batches, ingredient_batches, vessels, planned_packagings, drinks.
#   NO CURSOR IN SCHEMA  →  full sync (filter silently ignored if applied):
#     - fermentation_readings, ingredient_batch_stock_items_used.
#       (Verified by curl: ?created_at__gte=2030-01-01 still returns the full
#       count on these endpoints.)
_RESOURCES = [
    ("drinks",                          "/drinks/",                           None),
    ("drink_batches",                   "/drink-batches/",                    None),
    ("drink_batch_actions",             "/drink-batch-actions/",              "created_on"),
    ("drink_batch_stock_items_used",    "/drink-batch-stock-items-used/",     "created_at"),
    ("ingredient_batches",              "/ingredient-batches/",               None),
    ("ingredient_batch_actions",        "/ingredient-batch-actions/",         "created_at"),
    ("ingredient_batch_stock_items_used","/ingredient-batch-stock-items-used/",None),
    ("fermentation_readings",           "/fermentation-readings/",            None),
    ("vessels",                         "/vessels/",                          None),
    ("planned_packagings",              "/planned-packagings/",               None),
]


def _make_sync(table, endpoint, cursor_field):
    def _fn(config, state):
        sync_table(config, state, table=table, endpoint=endpoint,
                   cursor_field=cursor_field)
    _fn.__name__ = f"sync_{table}"
    return _fn


PRODUCTION_SYNCS = [_make_sync(t, e, c) for (t, e, c) in _RESOURCES]
