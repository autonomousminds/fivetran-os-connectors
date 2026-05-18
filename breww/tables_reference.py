"""Sync functions for Breww reference / lookup resources (small, full re-sync)."""

from helpers import sync_table

_RESOURCES = [
    ("business_details", "/business-details/", None),
    ("sites",            "/sites/",            None),
    ("locations",        "/locations/",        None),
    ("users",            "/users/",            None),
]


def _make_sync(table, endpoint, cursor_field):
    def _fn(config, state):
        sync_table(config, state, table=table, endpoint=endpoint,
                   cursor_field=cursor_field)
    _fn.__name__ = f"sync_{table}"
    return _fn


REFERENCE_SYNCS = [_make_sync(t, e, c) for (t, e, c) in _RESOURCES]
