"""
Sync logic for Productive reference/lookup tables.

All reference tables are fully synced on every run (small, stable datasets).
Relationship includes are derived from the official Productive API docs at
https://developer.productive.io/ and validated against the live API.
"""

from fivetran_connector_sdk import Logging as log

from api_client import fetch_all_pages
from helpers import mark_full_sync, should_full_sync, upsert


# ---------------------------------------------------------------------------
# Reference sync functions — validated includes per API docs
# ---------------------------------------------------------------------------

def sync_organizations(config, state):
    for record in fetch_all_pages(config, "/organizations"):
        upsert("organizations", record)


def sync_subsidiaries(config, state):
    for record in fetch_all_pages(config, "/subsidiaries",
                                  params={"include": "default_tax_rate,default_bank_account,default_document_type"}):
        upsert("subsidiaries", record)


def sync_custom_fields(config, state):
    for record in fetch_all_pages(config, "/custom_fields",
                                  params={"include": "project,section,survey"}):
        upsert("custom_fields", record)


def sync_custom_field_options(config, state):
    for record in fetch_all_pages(config, "/custom_field_options",
                                  params={"include": "custom_field"}):
        upsert("custom_field_options", record)


def sync_custom_field_sections(config, state):
    for record in fetch_all_pages(config, "/custom_field_sections"):
        upsert("custom_field_sections", record)


def sync_tags(config, state):
    for record in fetch_all_pages(config, "/tags"):
        upsert("tags", record)


def sync_service_types(config, state):
    for record in fetch_all_pages(config, "/service_types"):
        upsert("service_types", record)


def sync_deal_statuses(config, state):
    for record in fetch_all_pages(config, "/deal_statuses",
                                  params={"include": "pipeline"}):
        upsert("deal_statuses", record)


def sync_lost_reasons(config, state):
    for record in fetch_all_pages(config, "/lost_reasons"):
        upsert("lost_reasons", record)


def sync_workflows(config, state):
    for record in fetch_all_pages(config, "/workflows"):
        upsert("workflows", record)


def sync_workflow_statuses(config, state):
    for record in fetch_all_pages(config, "/workflow_statuses",
                                  params={"include": "workflow"}):
        upsert("workflow_statuses", record)


def sync_pipelines(config, state):
    for record in fetch_all_pages(config, "/pipelines",
                                  params={"include": "creator,updater"}):
        upsert("pipelines", record)


def sync_events(config, state):
    for record in fetch_all_pages(config, "/events"):
        upsert("events", record)


def sync_holiday_calendars(config, state):
    for record in fetch_all_pages(config, "/holiday_calendars"):
        upsert("holiday_calendars", record)


def sync_holidays(config, state):
    for record in fetch_all_pages(config, "/holidays",
                                  params={"include": "holiday_calendar,creator"}):
        upsert("holidays", record)


def sync_document_types(config, state):
    for record in fetch_all_pages(config, "/document_types",
                                  params={"include": "subsidiary,document_style"}):
        upsert("document_types", record)


def sync_document_styles(config, state):
    for record in fetch_all_pages(config, "/document_styles"):
        upsert("document_styles", record)


def sync_approval_policies(config, state):
    for record in fetch_all_pages(config, "/approval_policies"):
        upsert("approval_policies", record)


def sync_approval_workflows(config, state):
    for record in fetch_all_pages(config, "/approval_workflows",
                                  params={"include": "event,approval_policy"}):
        upsert("approval_workflows", record)


def sync_approval_policy_assignments(config, state):
    for record in fetch_all_pages(config, "/approval_policy_assignments",
                                  params={"include": "person,deal,approval_policy"}):
        upsert("approval_policy_assignments", record)


def sync_rate_cards(config, state):
    for record in fetch_all_pages(config, "/rate_cards",
                                  params={"include": "company,creator"}):
        upsert("rate_cards", record)


def sync_tax_rates(config, state):
    for record in fetch_all_pages(config, "/tax_rates"):
        upsert("tax_rates", record)


def sync_exchange_rates(config, state):
    if not should_full_sync(state, "exchange_rates"):
        log.info("Skipping exchange_rates — last full sync was within 7 days")
        return
    for record in fetch_all_pages(config, "/exchange_rates"):
        upsert("exchange_rates", record)
    mark_full_sync(state, "exchange_rates")


def sync_bank_accounts(config, state):
    for record in fetch_all_pages(config, "/bank_accounts"):
        upsert("bank_accounts", record)


def sync_invoice_templates(config, state):
    for record in fetch_all_pages(config, "/invoice_templates"):
        upsert("invoice_templates", record)


def sync_automatic_invoicing_rules(config, state):
    for record in fetch_all_pages(config, "/automatic_invoicing_rules",
                                  params={"include": "budget,creator"}):
        upsert("automatic_invoicing_rules", record)


def sync_payment_reminders(config, state):
    for record in fetch_all_pages(config, "/payment_reminders"):
        upsert("payment_reminders", record)


def sync_payment_reminder_sequences(config, state):
    for record in fetch_all_pages(config, "/payment_reminder_sequences",
                                  params={"include": "creator,updater"}):
        upsert("payment_reminder_sequences", record)


def sync_deal_cost_rates(config, state):
    for record in fetch_all_pages(config, "/deal_cost_rates",
                                  params={"include": "deal,person"}):
        upsert("deal_cost_rates", record)


def sync_kpd_codes(config, state):
    if not should_full_sync(state, "kpd_codes"):
        log.info("Skipping kpd_codes — last full sync was within 7 days")
        return
    for record in fetch_all_pages(config, "/kpd_codes"):
        upsert("kpd_codes", record)
    mark_full_sync(state, "kpd_codes")


def sync_report_categories(config, state):
    for record in fetch_all_pages(config, "/report_categories"):
        upsert("report_categories", record)


def sync_service_assignments(config, state):
    for record in fetch_all_pages(config, "/service_assignments",
                                  params={"include": "person,service"}):
        upsert("service_assignments", record)


def sync_service_type_assignments(config, state):
    for record in fetch_all_pages(config, "/service_type_assignments",
                                  params={"include": "person,service_type"}):
        upsert("service_type_assignments", record)


def sync_time_tracking_policies(config, state):
    for record in fetch_all_pages(config, "/time_tracking_policies",
                                  params={"include": "creator"}):
        upsert("time_tracking_policies", record)


def sync_teams(config, state):
    for record in fetch_all_pages(config, "/teams"):
        upsert("teams", record)


def sync_team_memberships(config, state):
    for record in fetch_all_pages(config, "/team_memberships",
                                  params={"include": "person,team"}):
        upsert("team_memberships", record)


def sync_sections(config, state):
    for record in fetch_all_pages(config, "/sections",
                                  params={"include": "deal"}):
        upsert("sections", record)


def sync_folders(config, state):
    for record in fetch_all_pages(config, "/folders"):
        upsert("folders", record)


def sync_organization_memberships(config, state):
    for record in fetch_all_pages(config, "/organization_memberships",
                                  params={"include": "person,user"}):
        upsert("organization_memberships", record)


def sync_integration_exporter_configurations(config, state):
    for record in fetch_all_pages(config, "/integration_exporter_configurations",
                                  params={"include": "company"}):
        upsert("integration_exporter_configurations", record)


def sync_integrations(config, state):
    for record in fetch_all_pages(config, "/integrations",
                                  params={"include": "subsidiary,project,creator,deal"}):
        upsert("integrations", record)


# ---------------------------------------------------------------------------
# Ordered list of all reference sync functions
# ---------------------------------------------------------------------------

REFERENCE_SYNCS = [
    sync_organizations,
    sync_subsidiaries,
    sync_custom_fields,
    sync_custom_field_options,
    sync_custom_field_sections,
    sync_tags,
    sync_service_types,
    sync_deal_statuses,
    sync_lost_reasons,
    sync_workflows,
    sync_workflow_statuses,
    sync_pipelines,
    sync_events,
    sync_holiday_calendars,
    sync_holidays,
    sync_document_types,
    sync_document_styles,
    sync_approval_policies,
    sync_approval_workflows,
    sync_approval_policy_assignments,
    sync_rate_cards,
    sync_tax_rates,
    sync_exchange_rates,
    sync_bank_accounts,
    sync_invoice_templates,
    sync_automatic_invoicing_rules,
    sync_payment_reminders,
    sync_payment_reminder_sequences,
    sync_deal_cost_rates,
    sync_kpd_codes,
    sync_report_categories,
    sync_service_assignments,
    sync_service_type_assignments,
    sync_time_tracking_policies,
    sync_teams,
    sync_team_memberships,
    sync_sections,
    sync_folders,
    sync_organization_memberships,
    sync_integration_exporter_configurations,
    sync_integrations,
]
