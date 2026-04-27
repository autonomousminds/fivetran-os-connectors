"""
Fivetran Custom Connector for Xero Accounting + UK Payroll.

Syncs all Xero Accounting API entities and all UK Payroll API entities
into Fivetran using the Connector SDK.
"""

import json

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import DailyRateLimitExceeded, reset_rate_limiter
from auth import is_payroll_available, reset_caches
from helpers import STATE_VERSION, validate_configuration
from schema_accounting import get_accounting_schema
from schema_payroll import get_payroll_schema
from tables_accounting import ACCOUNTING_INCREMENTAL_SYNCS, ACCOUNTING_REFERENCE_SYNCS
from tables_payroll import PAYROLL_DATA_SYNCS, PAYROLL_REFERENCE_SYNCS


def schema(configuration: dict) -> list:
    """Return combined schema for all Accounting + Payroll tables."""
    return get_accounting_schema() + get_payroll_schema()


def update(configuration: dict, state: dict):
    """
    Main sync function called by Fivetran on each sync run.

    Sync order:
    1. Accounting reference tables (full sync)
    2. Accounting incremental tables (If-Modified-Since / offset)
    3. Payroll reference tables (full sync)
    4. Payroll data tables (employees + sub-resources, pay runs, payslips, timesheets)
    """
    validate_configuration(configuration)

    # Reset module-level caches so stale tokens/scopes/rate-limiter state
    # don't carry over if the Python process is reused across sync runs
    reset_caches()
    reset_rate_limiter()

    # State versioning: reset cursors if state structure changes
    if state.get("_state_version") != STATE_VERSION:
        log.info(f"State version mismatch — resetting to v{STATE_VERSION}")
        state.clear()
        state["_state_version"] = STATE_VERSION

    log.info("Starting Xero connector sync...")

    all_syncs = [
        ("accounting reference", ACCOUNTING_REFERENCE_SYNCS),
        ("accounting", ACCOUNTING_INCREMENTAL_SYNCS),
    ]

    # Only include payroll syncs if the Xero app has payroll scopes authorised
    if is_payroll_available(configuration):
        all_syncs += [
            ("payroll reference", PAYROLL_REFERENCE_SYNCS),
            ("payroll", PAYROLL_DATA_SYNCS),
        ]
    else:
        log.warning("Payroll tables will be empty — payroll scopes not authorised.")

    for group_name, sync_list in all_syncs:
        for sync_fn in sync_list:
            table_name = sync_fn.__name__.replace("sync_", "")
            log.info(f"Syncing {group_name}: {table_name}")
            try:
                sync_fn(configuration, state)
            except DailyRateLimitExceeded:
                log.warning(
                    f"Xero daily rate limit exhausted during {table_name}. "
                    f"Saving progress and ending sync — remaining tables "
                    f"will be synced on the next run."
                )
                op.checkpoint(state)
                log.info("Xero connector sync complete (partial — daily limit reached).")
                return  # return normally so Fivetran commits all data synced so far
            except Exception as e:
                log.severe(f"Error syncing {table_name}: {e}")
            op.checkpoint(state)

    log.info("Xero connector sync complete.")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    import os
    import sys

    # Support --config <path> to load a JSON configuration file
    config_file = None
    if "--config" in sys.argv:
        idx = sys.argv.index("--config")
        if idx + 1 < len(sys.argv):
            config_file = sys.argv[idx + 1]

    if config_file:
        with open(config_file) as f:
            configuration = json.load(f)
    else:
        # Fall back to .env file for local development
        env_path = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        os.environ[key.strip()] = value.strip()

        configuration = {
            "client_id": os.environ.get("XERO_CLIENT_ID", ""),
            "client_secret": os.environ.get("XERO_CLIENT_SECRET", ""),
        }

    if not configuration.get("client_id") or not configuration.get("client_secret"):
        raise ValueError("Set client_id/client_secret via --config <file.json> or .env")

    connector.debug(configuration=configuration)
