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
from helpers import STATE_VERSION, validate_configuration
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
                log.severe(f"Rate limit exhausted during {table}: {e}")
                op.checkpoint(state)
                raise
            except Exception as e:
                log.severe(f"Error syncing {table}: {e}")
                # Continue to the next table — one failed resource shouldn't
                # block the rest of the sync.
        op.checkpoint(state)
        log.info(f"Checkpoint saved after {group_name} group.")

    log.info("Breww connector sync complete.")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
