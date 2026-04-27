"""
Fivetran Custom Connector for Productive.io.

Syncs all Productive API entities into Fivetran using the Connector SDK,
including salary data which the built-in Fivetran connector does not support.

Schema is built dynamically — only tables with data on the Productive API
are declared, so empty tables are never created in the destination.
"""

import json

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import RateLimitExceeded, fetch_all_pages
from helpers import STATE_VERSION, validate_configuration
from schema_data import get_data_schema
from schema_reference import get_reference_schema
from tables_data import DATA_FULL_SYNCS, DATA_INCREMENTAL_SYNCS
from tables_reference import REFERENCE_SYNCS

# Map Productive deleted_items.item_type to our table names for op.delete().
# Covers all tables from Fivetran's capture-deletes list plus our additional tables.
# The API may use singular or plural forms; we map both to be safe.
_TYPE_TO_TABLE = {
    # Core data tables
    "tasks": "tasks", "task": "tasks",
    "projects": "projects", "project": "projects",
    "deals": "deals", "deal": "deals",
    "companies": "companies", "company": "companies",
    "people": "people", "person": "people",
    "invoices": "invoices", "invoice": "invoices",
    "services": "services", "service": "services",
    "bookings": "bookings", "booking": "bookings",
    "time_entries": "time_entries", "time_entry": "time_entries",
    "expenses": "expenses", "expense": "expenses",
    "comments": "comments", "comment": "comments",
    "boards": "boards", "board": "boards",
    "task_lists": "task_lists", "task_list": "task_lists",
    "payments": "payments", "payment": "payments",
    "line_items": "line_items", "line_item": "line_items",
    "contact_entries": "contact_entries", "contact_entry": "contact_entries",
    "users": "users", "user": "users",
    # Reference tables (Fivetran capture-deletes parity)
    "custom_fields": "custom_fields", "custom_field": "custom_fields",
    "custom_field_options": "custom_field_options", "custom_field_option": "custom_field_options",
    "deal_statuses": "deal_statuses", "deal_status": "deal_statuses",
    "document_types": "document_types", "document_type": "document_types",
    "events": "events", "event": "events",
    "lost_reasons": "lost_reasons", "lost_reason": "lost_reasons",
    "organizations": "organizations", "organization": "organizations",
    "pipelines": "pipelines", "pipeline": "pipelines",
    "service_types": "service_types", "service_type": "service_types",
    "subsidiaries": "subsidiaries", "subsidiary": "subsidiaries",
    "workflows": "workflows", "workflow": "workflows",
    "workflow_statuses": "workflow_statuses", "workflow_status": "workflow_statuses",
    # Additional tables beyond Fivetran parity
    "salaries": "salaries", "salary": "salaries",
    "contracts": "contracts", "contract": "contracts",
    "attachments": "attachments", "attachment": "attachments",
    "memberships": "memberships", "membership": "memberships",
    "entitlements": "entitlements", "entitlement": "entitlements",
    "pages": "pages", "page": "pages",
    "todos": "todos", "todo": "todos",
    "discussions": "discussions", "discussion": "discussions",
    "proposals": "proposals", "proposal": "proposals",
    "purchase_orders": "purchase_orders", "purchase_order": "purchase_orders",
    "bills": "bills", "bill": "bills",
    "timers": "timers", "timer": "timers",
    "placeholders": "placeholders", "placeholder": "placeholders",
    "emails": "emails", "email": "emails",
    "surveys": "surveys", "survey": "surveys",
    "resource_requests": "resource_requests", "resource_request": "resource_requests",
    "widgets": "widgets", "widget": "widgets",
    "filters": "filters", "filter": "filters",
    "dashboards": "dashboards", "dashboard": "dashboards",
    "overheads": "overheads", "overhead": "overheads",
    "prices": "prices", "price": "prices",
}

def schema(configuration: dict) -> list:
    """Return schema for all tables with correct primary keys.

    All tables are always declared so that PKs are guaranteed correct.
    Empty tables in the destination are harmless; missing PK declarations
    cause Fivetran to fall back to _fivetran_id which corrupts upserts.
    """
    return get_reference_schema() + get_data_schema()


def update(configuration: dict, state: dict):
    """
    Main sync function called by Fivetran on each sync run.

    Sync order:
    1. Reference tables (full sync — small lookup/config data)
    2. Incremental data tables (cursor-based — time_entries, bookings, salaries, etc.)
    3. Full-sync data tables (people, companies, projects, tasks, etc.)
    """
    validate_configuration(configuration)

    # State versioning: reset cursors if state structure changes
    if state.get("_state_version") != STATE_VERSION:
        log.info(f"State version mismatch — resetting to v{STATE_VERSION}")
        state.clear()
        state["_state_version"] = STATE_VERSION

    log.info("Starting Productive connector sync...")

    all_syncs = [
        ("reference", REFERENCE_SYNCS),
        ("incremental", DATA_INCREMENTAL_SYNCS),
        ("full sync", DATA_FULL_SYNCS),
    ]

    for group_name, sync_list in all_syncs:
        for sync_fn in sync_list:
            table_name = sync_fn.__name__.replace("sync_", "")
            log.info(f"Syncing {group_name}: {table_name}")
            try:
                sync_fn(configuration, state)
            except RateLimitExceeded as e:
                log.severe(f"Rate limit exhausted during {table_name}: {e}")
                op.checkpoint(state)
                raise
            except Exception as e:
                log.severe(f"Error syncing {table_name}: {e}")
        # Checkpoint once per group to avoid excessive checkpointing
        op.checkpoint(state)
        log.info(f"Checkpoint saved after {group_name} group.")

    # Process deletions from the deleted_items endpoint
    _sync_deletions(configuration, state)

    log.info("Productive connector sync complete.")


def _sync_deletions(configuration, state):
    """Process soft deletes via the deleted_items endpoint."""
    cursor_key = "deleted_items_cursor"
    cursor = state.get(cursor_key)
    latest = cursor
    params = {}
    if cursor:
        params["filter[created_at]"] = cursor

    count = 0
    for record in fetch_all_pages(configuration, "/deleted_items", params=params):
        item_type = record.get("item_type", "")
        item_id = record.get("item_id")
        table = _TYPE_TO_TABLE.get(item_type)
        if table and item_id:
            op.delete(table=table, keys={"id": str(item_id)})
            count += 1
        deleted_at = record.get("deleted_at") or record.get("created_at") or ""
        if deleted_at and (not latest or deleted_at > latest):
            latest = deleted_at

    if latest and latest != cursor:
        state[cursor_key] = latest
    if count > 0:
        log.info(f"Processed {count} deletions.")
    op.checkpoint(state)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
