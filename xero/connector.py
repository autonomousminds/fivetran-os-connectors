"""
Fivetran Custom Connector for Xero Accounting + Reports + Files + Projects + UK Payroll.

Syncs Xero data into Fivetran using the Connector SDK with the new
granular OAuth scope structure (Xero migrated from broad scopes
`accounting.transactions` / `accounting.reports.read` to ~14 granular
scopes effective 2 Mar 2026; all apps must migrate by Sep 2027).

Sync groups (gated by scope-probing — denied scopes degrade gracefully):
  1. Accounting reference tables (full sync)
  2. Accounting incremental tables (If-Modified-Since / offset)
  3. Journals / general ledger — premium-gated (Advanced tier post-April 2026)
  4. Reports — Trial Balance, Balance Sheet, P&L, etc.
  5. Files API
  6. Projects API
  7. UK Payroll
"""

import json

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import DailyRateLimitExceeded, reset_rate_limiter
from auth import (
    is_assets_available,
    is_files_available,
    is_journals_available,
    is_payroll_available,
    is_projects_available,
    reset_caches,
)
from helpers import STATE_VERSION, validate_configuration
from schema_accounting import get_accounting_schema
from schema_files import get_files_schema
from schema_payroll import get_payroll_schema
from schema_projects import get_projects_schema
from schema_reports import get_reports_schema
from tables_accounting import (
    ACCOUNTING_INCREMENTAL_SYNCS,
    ACCOUNTING_REFERENCE_SYNCS,
    ASSETS_SYNCS,
    JOURNALS_SYNCS,
)
from tables_files import FILES_SYNCS
from tables_payroll import PAYROLL_DATA_SYNCS, PAYROLL_REFERENCE_SYNCS
from tables_projects import PROJECTS_SYNCS
from tables_reports import REPORT_SYNCS


def schema(configuration: dict) -> list:
    """Combined schema across every Xero API surface this connector covers."""
    return (
        get_accounting_schema()
        + get_reports_schema()
        + get_files_schema()
        + get_projects_schema()
        + get_payroll_schema()
    )


def _run_sync_group(group_name: str, sync_list: list, configuration: dict, state: dict) -> bool:
    """Run every sync in a group with shared error handling.
    Returns False when a DailyRateLimitExceeded halts the run (caller should exit early)."""
    for sync_fn in sync_list:
        table_name = sync_fn.__name__.replace("sync_", "")
        log.info(f"Syncing {group_name}: {table_name}")
        try:
            sync_fn(configuration, state)
        except DailyRateLimitExceeded:
            log.warning(
                f"Xero daily rate limit exhausted during {table_name}. "
                f"Saving progress and ending sync — remaining tables will be "
                f"synced on the next run."
            )
            op.checkpoint(state)
            return False
        except Exception as e:
            log.severe(f"Error syncing {table_name}: {e}")
        op.checkpoint(state)
    return True


def update(configuration: dict, state: dict):
    """Main sync function called by Fivetran on each sync run."""
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

    sync_groups: list = [
        ("accounting reference", ACCOUNTING_REFERENCE_SYNCS),
        ("accounting", ACCOUNTING_INCREMENTAL_SYNCS),
    ]

    # Feature-availability probes may hit Xero's daily 5000-call limit
    # if a previous sync this calendar day already exhausted it. Catching
    # the exception lets the connector exit gracefully and resume next day
    # rather than crashing with an uncaught exception that aborts state
    # persistence.
    try:
        # Fixed Assets — gated by Xero feature provisioning, not just scope grant
        if is_assets_available(configuration):
            sync_groups.append(("assets", ASSETS_SYNCS))
        else:
            log.warning("Assets tables will be empty — Fixed Assets not enabled for this org.")

        # Journals: scope-probed because accounting.journals.read is premium-gated
        # (Advanced tier + Xero security/use-case approval) post-April 2026.
        if is_journals_available(configuration):
            sync_groups.append(("journals", JOURNALS_SYNCS))
        else:
            log.warning(
                "Journals (general ledger) sync skipped — accounting.journals.read "
                "not authorised. To enable, customer must be on Xero Advanced tier "
                "and complete Xero's security assessment + use-case approval."
            )

        # Reports: every granular reports.* scope bundles into the 'accounting'
        # token. Individual report syncs will warn-and-continue on denial, so we
        # always include the group.
        sync_groups.append(("reports", REPORT_SYNCS))

        # Files API — separate token + scope
        if is_files_available(configuration):
            sync_groups.append(("files", FILES_SYNCS))
        else:
            log.warning("Files tables will be empty — files.read not authorised.")

        # Projects API — separate token + scope
        if is_projects_available(configuration):
            sync_groups.append(("projects", PROJECTS_SYNCS))
        else:
            log.warning("Projects tables will be empty — projects.read not authorised.")

        # Payroll (UK) — separate token + scope, also gated by org provisioning
        if is_payroll_available(configuration):
            sync_groups += [
                ("payroll reference", PAYROLL_REFERENCE_SYNCS),
                ("payroll", PAYROLL_DATA_SYNCS),
            ]
        else:
            log.warning("Payroll tables will be empty — payroll scopes not authorised or org has no Payroll.")
    except DailyRateLimitExceeded as e:
        # Xero's day limit is rolling 24h. Retry-After (embedded in the
        # exception message) is the seconds until the oldest call in the
        # window expires — could be minutes or many hours depending on
        # when the budget got burned. Aborting now is safe regardless of
        # the value because all state is already checkpointed.
        log.warning(
            f"Daily rate limit hit during feature probing: {e}. "
            f"Aborting sync — will resume when Fivetran reschedules."
        )
        op.checkpoint(state)
        log.info("Xero connector sync complete (partial — daily limit reached).")
        return

    for group_name, sync_list in sync_groups:
        if not _run_sync_group(group_name, sync_list, configuration, state):
            log.info("Xero connector sync complete (partial — daily limit reached).")
            return

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
