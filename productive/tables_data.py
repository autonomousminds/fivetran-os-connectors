"""
Sync logic for Productive data tables.

Tables are split into:
- Incremental: use date-based filters (after/updated_at) with cursor state
- Full sync: re-fetch all records each run

Each table specifies which relationships to include via the JSON:API `include`
parameter so that foreign key IDs are populated in the flattened records.

Relationship includes are derived from the official Productive API docs at
https://developer.productive.io/ — each endpoint's documented relationships
were validated against the live API to avoid 400 errors.
"""

from datetime import datetime, timedelta, timezone

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import fetch_all_pages
from helpers import upsert


# Trailing-window (in days) used for tables where the API has no usable
# "updated since" filter. Records inside this window are fully re-synced
# on every run so backdated edits/entries are caught. Anything older is
# assumed immutable (deletions still come via /deleted_items).
TIME_ENTRIES_TRAILING_DAYS = 90


# ---------------------------------------------------------------------------
# Per-table relationship includes — validated against API docs + live API
# Only includable relationships that produce useful FK columns are listed.
# ---------------------------------------------------------------------------

INCLUDES = {
    # Incremental tables
    "time_entries": "person,service,task,approver,updater,rejecter,creator,timesheet,invoice_attribution",
    "time_entry_versions": "creator,service",
    "bookings": "service,event,person,creator,updater,approver,rejecter,canceler,origin",
    "services": "service_type,deal,person,section",
    "activities": "creator",
    "salaries": "person,holiday_calendar",

    # Full-sync tables
    "people": "company,manager,subsidiary",
    "users": "",
    "companies": "default_subsidiary,default_tax_rate,parent_company",
    "projects": "company,project_manager,workflow",
    "boards": "project",
    "task_lists": "project,board,folder",
    "tasks": "project,creator,assignee,task_list,workflow_status,parent_task",
    "task_dependencies": "",
    "deals": "company,responsible,deal_status,project,pipeline,subsidiary,contract,lost_reason,creator",
    "contracts": "template",
    "invoices": "company,creator,subsidiary,document_type,parent_invoice",
    "invoice_attributions": "invoice,budget",
    "line_items": "invoice,service,expense,service_type,tax_rate,kpd_code",
    "payments": "invoice",
    "expenses": "deal,person,service,approver,vendor,service_type,tax_rate",
    "memberships": "person,team,project,deal,page,dashboard,filter",
    "comments": "creator,task,deal,project",
    "attachments": "creator,task,deal",
    "contact_entries": "company,person,invoice,subsidiary",
    "pages": "creator,project,parent_page,root_page",
    "page_versions": "page",
    "dashboards": "creator,project",
    "filters": "creator,report_category",
    "prices": "service_type,company,rate_card,updater",
    "entitlements": "event,person,approval_workflow",
    "timers": "time_entry",
    "timesheets": "person,creator",
    "overheads": "subsidiary",
    "revenue_distributions": "deal,creator",
    "placeholders": "project",
    "purchase_orders": "vendor,deal,creator,document_type",
    "bills": "purchase_order,creator,deal",
    "todos": "assignee,deal,task",
    "discussions": "page",
    "proposals": "deal,creator,subsidiary",
    "emails": "creator,deal,invoice",
    "deleted_items": "deleter",
    "surveys": "project,creator",
    "survey_fields": "survey",
    "survey_field_options": "survey_field",
    "survey_responses": "survey,creator",
    "resource_requests": "service,creator",
    "pulses": "creator,filter",
    "widgets": "dashboard,filter",
}


# ---------------------------------------------------------------------------
# Incremental sync helpers
# ---------------------------------------------------------------------------

def _sync_incremental(config, state, table, endpoint, filter_key, cursor_key,
                      checkpoint_every=500):
    """
    Generic incremental sync using a date-based filter.

    Reads cursor from state, passes as filter param, tracks latest timestamp,
    and writes updated cursor back to state. Checkpoints every checkpoint_every
    records to save progress and reduce memory pressure on large tables.

    Uses page_size=50 to keep per-request memory low (JSON:API includes
    embed full related objects, which can bloat responses significantly).
    """
    cursor = state.get(cursor_key)
    latest = cursor
    params = {}
    if cursor:
        params[f"filter[{filter_key}]"] = cursor

    includes = INCLUDES.get(table)
    if includes:
        params["include"] = includes

    # Use smaller page size for tables with many includes to reduce memory
    num_includes = len(includes.split(",")) if includes else 0
    page_size = 50 if num_includes >= 4 else 100

    count = 0
    for record in fetch_all_pages(config, endpoint, params=params, page_size=page_size):
        updated = record.get("updated_at") or record.get("created_at") or ""
        if updated and (not latest or updated > latest):
            latest = updated
        upsert(table, record)
        count += 1
        # Periodic checkpoint to save progress on large tables
        if count % checkpoint_every == 0:
            if latest:
                state[cursor_key] = latest
            op.checkpoint(state)
            log.info(f"{table}: checkpointed at {count} records (cursor={latest})")

    if latest and latest != cursor:
        state[cursor_key] = latest
        log.info(f"Updated cursor {cursor_key} = {latest}")

    if count > 0:
        log.info(f"{table}: synced {count} records")


def _sync_trailing_window(config, state, table, endpoint, filter_key, days,
                          checkpoint_every=500):
    """
    Trailing-window sync for endpoints whose only date filter operates on the
    record's business date (e.g. /time_entries `filter[after]` filters by
    `date`, not `updated_at`). Such endpoints silently drop backdated entries
    when driven by an updated_at cursor, because new/edited rows can have a
    `date` far in the past.

    Strategy: ignore any stored cursor and always re-fetch the last `days`
    days on every run. Records outside the window are assumed immutable.
    Deletions inside or outside the window are still handled by the
    /deleted_items endpoint in connector._sync_deletions.
    """
    floor = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()
    params = {f"filter[{filter_key}]": floor}

    includes = INCLUDES.get(table)
    if includes:
        params["include"] = includes

    num_includes = len(includes.split(",")) if includes else 0
    page_size = 50 if num_includes >= 4 else 100

    log.info(f"{table}: trailing-window sync from {floor} ({days}d)")
    count = 0
    for record in fetch_all_pages(config, endpoint, params=params, page_size=page_size):
        upsert(table, record)
        count += 1
        if count % checkpoint_every == 0:
            op.checkpoint(state)
            log.info(f"{table}: checkpointed at {count} records")

    if count > 0:
        log.info(f"{table}: synced {count} records")


def _sync_full(config, state, table, endpoint):
    """Generic full sync — fetch all records and upsert."""
    params = {}
    includes = INCLUDES.get(table)
    if includes:
        params["include"] = includes

    for record in fetch_all_pages(config, endpoint, params=params if params else None):
        upsert(table, record)


# ---------------------------------------------------------------------------
# Incremental sync functions
# ---------------------------------------------------------------------------

def sync_time_entries(config, state):
    # Productive's /time_entries has no "updated since" filter — filter[after]
    # operates on the business `date`, so a cursor keyed on updated_at drops
    # any backdated entries. Use a trailing-window re-sync instead.
    _sync_trailing_window(config, state, "time_entries", "/time_entries",
                          filter_key="after", days=TIME_ENTRIES_TRAILING_DAYS)


def sync_time_entry_versions(config, state):
    _sync_incremental(config, state, "time_entry_versions", "/time_entry_versions",
                      filter_key="created_at", cursor_key="time_entry_versions_cursor")


def sync_bookings(config, state):
    _sync_incremental(config, state, "bookings", "/bookings",
                      filter_key="updated_at", cursor_key="bookings_cursor")


def sync_services(config, state):
    _sync_incremental(config, state, "services", "/services",
                      filter_key="after", cursor_key="services_cursor")


def sync_activities(config, state):
    _sync_incremental(config, state, "activities", "/activities",
                      filter_key="after", cursor_key="activities_cursor")


def sync_salaries(config, state):
    """Sync salaries — the key addition over the built-in Fivetran connector."""
    _sync_incremental(config, state, "salaries", "/salaries",
                      filter_key="after", cursor_key="salaries_cursor")


# ---------------------------------------------------------------------------
# Full sync functions
# ---------------------------------------------------------------------------

def sync_people(config, state):
    _sync_incremental(config, state, "people", "/people",
                      filter_key="last_activity_at", cursor_key="people_cursor")


def sync_users(config, state):
    _sync_full(config, state, "users", "/users")


def sync_companies(config, state):
    _sync_incremental(config, state, "companies", "/companies",
                      filter_key="last_activity_at", cursor_key="companies_cursor")


def sync_projects(config, state):
    _sync_full(config, state, "projects", "/projects")


def sync_boards(config, state):
    _sync_full(config, state, "boards", "/boards")


def sync_task_lists(config, state):
    _sync_full(config, state, "task_lists", "/task_lists")


def sync_tasks(config, state):
    _sync_incremental(config, state, "tasks", "/tasks",
                      filter_key="updated_at", cursor_key="tasks_cursor")


def sync_task_dependencies(config, state):
    _sync_full(config, state, "task_dependencies", "/task_dependencies")


def sync_deals(config, state):
    _sync_incremental(config, state, "deals", "/deals",
                      filter_key="last_activity_at", cursor_key="deals_cursor")


def sync_contracts(config, state):
    _sync_full(config, state, "contracts", "/contracts")


def sync_invoices(config, state):
    _sync_incremental(config, state, "invoices", "/invoices",
                      filter_key="last_activity_at", cursor_key="invoices_cursor")


def sync_invoice_attributions(config, state):
    _sync_full(config, state, "invoice_attributions", "/invoice_attributions")


def sync_line_items(config, state):
    _sync_full(config, state, "line_items", "/line_items")


def sync_payments(config, state):
    _sync_incremental(config, state, "payments", "/payments",
                      filter_key="paid_after", cursor_key="payments_cursor")


def sync_expenses(config, state):
    _sync_incremental(config, state, "expenses", "/expenses",
                      filter_key="created_at", cursor_key="expenses_cursor")


def sync_memberships(config, state):
    _sync_full(config, state, "memberships", "/memberships")


def sync_comments(config, state):
    _sync_full(config, state, "comments", "/comments")


def sync_attachments(config, state):
    _sync_full(config, state, "attachments", "/attachments")


def sync_contact_entries(config, state):
    _sync_full(config, state, "contact_entries", "/contact_entries")


def sync_pages(config, state):
    _sync_full(config, state, "pages", "/pages")


def sync_page_versions(config, state):
    _sync_full(config, state, "page_versions", "/page_versions")


def sync_dashboards(config, state):
    _sync_full(config, state, "dashboards", "/dashboards")


def sync_filters(config, state):
    _sync_full(config, state, "filters", "/filters")


def sync_prices(config, state):
    _sync_full(config, state, "prices", "/prices")


def sync_entitlements(config, state):
    _sync_full(config, state, "entitlements", "/entitlements")


def sync_timers(config, state):
    _sync_full(config, state, "timers", "/timers")


def sync_timesheets(config, state):
    _sync_full(config, state, "timesheets", "/timesheets")


def sync_overheads(config, state):
    _sync_full(config, state, "overheads", "/overheads")


def sync_revenue_distributions(config, state):
    _sync_full(config, state, "revenue_distributions", "/revenue_distributions")


def sync_placeholders(config, state):
    _sync_full(config, state, "placeholders", "/placeholders")


def sync_purchase_orders(config, state):
    _sync_full(config, state, "purchase_orders", "/purchase_orders")


def sync_bills(config, state):
    _sync_full(config, state, "bills", "/bills")


def sync_todos(config, state):
    _sync_full(config, state, "todos", "/todos")


def sync_discussions(config, state):
    _sync_full(config, state, "discussions", "/discussions")


def sync_proposals(config, state):
    _sync_full(config, state, "proposals", "/proposals")


def sync_emails(config, state):
    _sync_full(config, state, "emails", "/emails")


def sync_deleted_items(config, state):
    _sync_full(config, state, "deleted_items", "/deleted_items")


def sync_surveys(config, state):
    _sync_full(config, state, "surveys", "/surveys")


def sync_survey_fields(config, state):
    _sync_full(config, state, "survey_fields", "/survey_fields")


def sync_survey_field_options(config, state):
    _sync_full(config, state, "survey_field_options", "/survey_field_options")


def sync_survey_responses(config, state):
    _sync_full(config, state, "survey_responses", "/survey_responses")


def sync_resource_requests(config, state):
    _sync_full(config, state, "resource_requests", "/resource_requests")


def sync_pulses(config, state):
    _sync_full(config, state, "pulses", "/pulses")


def sync_widgets(config, state):
    _sync_full(config, state, "widgets", "/widgets")


# ---------------------------------------------------------------------------
# Ordered sync lists
# ---------------------------------------------------------------------------

DATA_INCREMENTAL_SYNCS = [
    sync_time_entries,
    sync_time_entry_versions,
    sync_bookings,
    sync_services,
    sync_activities,
    sync_salaries,
    sync_people,
    sync_companies,
    sync_tasks,
    sync_deals,
    sync_invoices,
    sync_expenses,
    sync_payments,
]

DATA_FULL_SYNCS = [
    sync_users,
    sync_projects,
    sync_boards,
    sync_task_lists,
    sync_task_dependencies,
    sync_contracts,
    sync_invoice_attributions,
    sync_line_items,
    sync_memberships,
    sync_comments,
    sync_attachments,
    sync_contact_entries,
    sync_pages,
    sync_page_versions,
    sync_dashboards,
    sync_filters,
    sync_prices,
    sync_entitlements,
    sync_timers,
    sync_timesheets,
    sync_overheads,
    sync_revenue_distributions,
    sync_placeholders,
    sync_purchase_orders,
    sync_bills,
    sync_todos,
    sync_discussions,
    sync_proposals,
    sync_emails,
    sync_deleted_items,
    sync_surveys,
    sync_survey_fields,
    sync_survey_field_options,
    sync_survey_responses,
    sync_resource_requests,
    sync_pulses,
    sync_widgets,
]
