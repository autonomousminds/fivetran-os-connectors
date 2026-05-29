"""
Fivetran Custom Connector for Zoho Creator (https://www.zoho.com/creator/).

Syncs every application, form, report, and record the OAuth grant can see.
See README.md for the OAuth Self-Client setup and the list of caveats.
"""

import json

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import DailyLimitExceeded, reset_rate_limiter
from auth import reset_caches as reset_auth_caches
from helpers import (
    STATE_VERSION,
    log_orphan_diagnostics,
    reset_tracking,
    validate_configuration,
)
from schema import get_schema, reset_discovery_cache
from tables_data import sync_all_data
from tables_meta import sync_meta_all


def schema(configuration: dict) -> list:
    """Return the dynamic table list (4 fixed meta + N data + M subform tables)."""
    return get_schema(configuration)


def update(configuration: dict, state: dict):
    validate_configuration(configuration)
    state = state or {}

    # Reset all module-level caches so a re-used Python process can't serve
    # stale tokens, rate-limiter timestamps, or discovery results.
    reset_auth_caches()
    reset_rate_limiter()
    reset_tracking()
    reset_discovery_cache()

    if state.get("_state_version") != STATE_VERSION:
        log.info(f"State version mismatch (have {state.get('_state_version')}, "
                 f"want {STATE_VERSION}) — resetting state.")
        state.clear()
        state["_state_version"] = STATE_VERSION

    log.info("Zoho Creator connector starting...")

    # 1. Meta tables — small, always full-sync; also primes the discovery cache
    #    so all downstream syncs share the same metadata snapshot.
    try:
        sync_meta_all(configuration, state)
        op.checkpoint(state)
    except DailyLimitExceeded as e:
        log.severe(f"Daily limit hit during meta sync: {e}")
        op.checkpoint(state)
        return
    except Exception as e:
        # Meta sync is foundational. If it fails completely we don't know
        # which apps/forms to sync, so bail.
        log.severe(f"Meta sync failed: {e!r}. Aborting run.")
        op.checkpoint(state)
        raise

    # 2. Data tables — one per form
    try:
        sync_all_data(configuration, state)
    except DailyLimitExceeded as e:
        log.warning(
            f"Daily API limit reached during data sync: {e}. "
            f"Checkpointing — Fivetran will resume on next run."
        )
        op.checkpoint(state)
        return

    # 3. Diagnostics on unresolved cross-app lookup references.
    try:
        log_orphan_diagnostics()
    except Exception as e:
        log.warning(f"Orphan diagnostics raised {e!r} — non-fatal")

    op.checkpoint(state)
    log.info("Zoho Creator connector sync complete.")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
