"""
Fivetran Custom Connector for Breww (https://breww.com).

Syncs all 40 user-facing resources from the Breww public REST API into Fivetran
using the Connector SDK. Bearer-token authentication, DRF page-number pagination,
and a mix of incremental (last_modified_at / created_at / created_on) and full-resync
strategies depending on what each endpoint supports.
"""

import json

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import RateLimitExceeded
from helpers import STATE_VERSION, recover_orphans, reset_tracking, validate_configuration
from schema import get_schema
from tables_commercial import COMMERCIAL_SYNCS
from tables_inventory import INVENTORY_SYNCS
from tables_production import PRODUCTION_SYNCS
from tables_reference import REFERENCE_SYNCS


def schema(configuration: dict) -> list:
    """Return schema for all 40 Breww tables. Every PK is `id`."""
    return get_schema()


def update(configuration: dict, state: dict):
    """Main sync function called by Fivetran on each run.

    Sync order:
      1. Reference tables  — small lookup data (business_details, sites, locations, users)
      2. Commercial / CRM  — orders (the only true updated-since), customers, payments, etc.
      3. Inventory / supply
      4. Production
    """
    validate_configuration(configuration)

    state = state or {}
    if state.get("_state_version") != STATE_VERSION:
        log.info(f"State version mismatch — resetting to v{STATE_VERSION}")
        state.clear()
        state["_state_version"] = STATE_VERSION

    log.info("Starting Breww connector sync...")
    reset_tracking()  # clear in-memory orphan-recovery counters

    all_groups = [
        ("reference",  REFERENCE_SYNCS),
        ("commercial", COMMERCIAL_SYNCS),
        ("inventory",  INVENTORY_SYNCS),
        ("production", PRODUCTION_SYNCS),
    ]

    for group_name, syncs in all_groups:
        for sync_fn in syncs:
            table = sync_fn.__name__.replace("sync_", "")
            log.info(f"Syncing {group_name}: {table}")
            try:
                sync_fn(configuration, state)
            except RateLimitExceeded as e:
                # Daily quota exhausted — checkpoint and return cleanly so
                # Fivetran logs a successful run. The next scheduled sync
                # picks up from the last checkpoint once quota resets.
                log.warning(f"Daily quota exhausted during {table}: {e}")
                op.checkpoint(state)
                log.info("Breww connector sync ending early (quota exhausted) — will resume on next scheduled run.")
                return
            except Exception as e:
                log.severe(f"Error syncing {table}: {e}")
                # Continue to the next table — one failed resource shouldn't
                # block the rest of the sync.
        op.checkpoint(state)
        log.info(f"Checkpoint saved after {group_name} group.")

    # Recover records hidden from list endpoints (soft-deleted customers /
    # ex-employee users) by fetching each referenced FK id from its detail
    # endpoint. Without this, ~30% of orders reference customers that aren't
    # in the customers_suppliers table.
    log.info("Starting orphan-recovery pass...")
    try:
        recover_orphans(configuration, state)
    except RateLimitExceeded as e:
        log.warning(f"Daily quota exhausted during orphan recovery: {e}")
        op.checkpoint(state)
        log.info("Breww connector sync ending early (quota exhausted) — will resume on next scheduled run.")
        return
    except Exception as e:
        # Orphan recovery is best-effort — don't fail the whole sync if it errors.
        log.severe(f"Orphan recovery failed: {e}")

    log.info("Breww connector sync complete.")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
