"""Sync functions for Breww production resources (drinks, batches, vessels, fermentation)."""

from helpers import sync_table

_RESOURCES = [
    ("drinks",                          "/drinks/",                           None),
    ("drink_batches",                   "/drink-batches/",                    "created_on"),
    ("drink_batch_actions",             "/drink-batch-actions/",              "created_on"),
    ("drink_batch_stock_items_used",    "/drink-batch-stock-items-used/",     "created_at"),
    ("ingredient_batches",              "/ingredient-batches/",               None),
    ("ingredient_batch_actions",        "/ingredient-batch-actions/",         "created_at"),
    ("ingredient_batch_stock_items_used","/ingredient-batch-stock-items-used/",None),
    ("fermentation_readings",           "/fermentation-readings/",            None),
    ("vessels",                         "/vessels/",                          None),
    ("planned_packagings",              "/planned-packagings/",               "created_at"),
]


def _make_sync(table, endpoint, cursor_field):
    def _fn(config, state):
        sync_table(config, state, table=table, endpoint=endpoint,
                   cursor_field=cursor_field)
    _fn.__name__ = f"sync_{table}"
    return _fn


PRODUCTION_SYNCS = [_make_sync(t, e, c) for (t, e, c) in _RESOURCES]
