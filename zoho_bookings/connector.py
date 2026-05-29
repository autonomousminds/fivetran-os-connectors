"""
Fivetran Connector SDK entry point for Zoho Bookings.

Strategy: pure full re-sync each run over a configurable date window. Captures
status changes (cancellations, reschedules, completions) at the cost of higher
API quota usage.

Tables: workspaces, services, staff, resources, appointments, and four bridge
tables for the service↔staff / service↔workspace / staff↔service /
staff↔workspace many-to-many relationships.
"""

import json

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

import auth
import api_client
import helpers
import schema as schema_module
import tables_data
import tables_meta


def update(configuration: dict, state: dict):
    helpers.validate_configuration(configuration)

    # Fresh per-run caches — required because Fivetran reuses the Python
    # process across invocations.
    auth.reset_caches()
    api_client.reset_rate_limiter()
    helpers.reset_tracking()

    log.info("Zoho Bookings sync starting")
    try:
        workspace_ids = tables_meta.sync_meta_all(configuration, state)
        tables_data.sync_appointments(configuration, state, workspace_ids)
        op.checkpoint(state=state)
        log.info("Zoho Bookings sync complete")
    except api_client.DailyLimitExceeded as exc:
        # Soft exit: checkpoint whatever we have so the next run resumes
        # without redoing work.
        log.severe(str(exc))
        op.checkpoint(state=state)


def schema(configuration: dict):
    return schema_module.get_schema(configuration)


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
