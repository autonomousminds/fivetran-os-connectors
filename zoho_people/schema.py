"""
Schema for the Zoho People connector.

The schema is a mix of fixed + dynamic tables:

## Fixed tables (always present)

  forms_meta              one row per form in the org
  views_meta              one row per (form, view)
  form_fields             one row per (form, field) — best-effort discovery
  file_categories         file/document categories
  files                   files index (HR + company)
  holidays                org-wide holiday calendar
  leave_types             distinct leave types (derived from leave records)
  attendance_daily        daily attendance summary per employee
  leave_records           one row per leave application/record
  leave_balance           booked + balance per (employee, leave_type, period)
  jobs                    timetracker jobs
  timelogs                timetracker time log entries
  courses                 LMS courses (only if sync_lms_courses=true)

## Dynamic tables

For each form discovered via `/people/api/forms`, one data table:

    form_{safe_form_link_name}     primary key: record_id

This dynamically covers the standard HR forms — employees (`P_employee`),
departments (`P_Department`), designations, locations, dependants,
qualifications, work experience, asset records, expense forms, etc. —
plus any custom forms the customer has built.

Records contain `tabularSections` (sub-form arrays). These are extracted
into child tables created lazily at sync time:

    form_{safe_form_link_name}__sub_{section_name}    primary key: ID

Child tables are NOT pre-declared here — the SDK creates them on the
first upsert from real data and infers columns automatically.
"""

import re

from fivetran_connector_sdk import Logging as log


_discovery_cache: dict = {}


def reset_discovery_cache():
    global _discovery_cache
    _discovery_cache = {}


_SAFE_RE = re.compile(r"[^a-z0-9]+")


def _safe(name: str) -> str:
    return _SAFE_RE.sub("_", (name or "").lower()).strip("_")


def table_for_form(form_link_name: str) -> str:
    """Stable, warehouse-safe table name for a form. Prefix with `form_`
    so it never collides with a built-in module table."""
    return f"form_{_safe(form_link_name)}"


def child_table_for_subform(form_link_name: str, section_name: str) -> str:
    return f"{table_for_form(form_link_name)}__sub_{_safe(section_name)}"


# ── Discovery ────────────────────────────────────────────────────────────────
def discover(configuration: dict) -> dict:
    """Walk the org's forms via `/people/api/forms` and the views per form
    via `/api/forms/{linkName}/views`. Return one cached catalog:

        {
          "forms": [
            {
              "form_link_name": "P_employee",
              "display_name":   "Employee",
              "is_custom":      False,
              "component_id":   123,
              "raw_meta":       {...},
              "default_view":   "P_EmployeeView",
              "views": [
                {"view_id": ..., "view_name": "P_EmployeeView",
                 "display_name": "Employee View", "is_default": True,
                 "view_type": ..., "table": "form_p_employee"},
                ...
              ],
            },
            ...
          ]
        }

    The `default_view` is what the data-sync layer uses to fetch records
    via the `/api/forms/{view}/records` endpoint (the only one that
    supports the `modifiedtime` incremental filter).
    """
    global _discovery_cache
    if _discovery_cache:
        return _discovery_cache

    from api_client import api_request, unwrap_envelope, ScopeMissing
    from auth import api_host

    host = api_host(configuration)
    log.info("Discovering Zoho People schema (forms → views)...")

    # 1) List forms — `GET /people/api/forms`
    try:
        forms_body = api_request(configuration, f"{host}/people/api/forms")
    except ScopeMissing as e:
        log.severe(
            f"Cannot list forms — ZOHOPEOPLE.forms.READ scope is missing "
            f"from the refresh token. Add it in the Zoho API console and "
            f"regenerate the refresh_token. Detail: {e!s}"
        )
        _discovery_cache = {"forms": []}
        return _discovery_cache

    # Two response shapes documented for this endpoint:
    #   (a) {"response": {"result": [...forms...], "status": 0, ...}}
    #   (b) Bare array (some newer surfaces).
    raw_forms = None
    if isinstance(forms_body, list):
        raw_forms = forms_body
    elif isinstance(forms_body, dict):
        inner = forms_body.get("response", forms_body)
        if isinstance(inner, dict):
            raw_forms = inner.get("result") or inner.get("forms") or []
        elif isinstance(inner, list):
            raw_forms = inner

    if not isinstance(raw_forms, list):
        log.warning(f"/people/api/forms returned unexpected shape: "
                    f"{type(forms_body).__name__} — treating as empty")
        raw_forms = []

    log.info(f"  /people/api/forms → {len(raw_forms)} form(s)")

    forms_out = []
    # `skip_forms` is configured as a comma-separated string (Fivetran SDK
    # requires all config values to be strings). Empty string → no skips.
    skip_raw = configuration.get("skip_forms") or ""
    if isinstance(skip_raw, list):
        skip_set = set(skip_raw)  # tolerate legacy list form
    else:
        skip_set = {s.strip() for s in str(skip_raw).split(",") if s.strip()}

    for raw in raw_forms:
        if not isinstance(raw, dict):
            continue
        form_link = (raw.get("formLinkName") or raw.get("form_link_name")
                     or raw.get("linkName"))
        if not form_link:
            log.fine(f"  Skipping form with no link name: {raw}")
            continue
        if form_link in skip_set:
            log.info(f"  Skipping form (per config): {form_link}")
            continue

        # 2) Per-form views — `GET /api/forms/{link}/views`
        try:
            views_body = api_request(configuration, f"{host}/api/forms/{form_link}/views")
        except ScopeMissing:
            views_body = {}
        except Exception as e:
            log.warning(f"  views fetch failed for {form_link}: {e!r}")
            views_body = {}

        raw_views = []
        if isinstance(views_body, list):
            raw_views = views_body
        elif isinstance(views_body, dict):
            inner = views_body.get("response", views_body)
            if isinstance(inner, dict):
                raw_views = inner.get("result") or inner.get("views") or []
            elif isinstance(inner, list):
                raw_views = inner

        views_out = []
        default_view_name = None
        for vraw in (raw_views or []):
            if not isinstance(vraw, dict):
                continue
            view_name = vraw.get("viewName") or vraw.get("view_name") or vraw.get("name")
            if not view_name:
                continue
            is_default = bool(
                vraw.get("isDefaultView") or vraw.get("is_default_view")
                or vraw.get("isDefault")
            )
            if is_default and not default_view_name:
                default_view_name = view_name
            views_out.append({
                "view_id":      vraw.get("viewId") or vraw.get("view_id"),
                "view_name":    view_name,
                "display_name": vraw.get("displayName") or vraw.get("viewdisplayName"),
                "is_default":   is_default,
                "view_type":    vraw.get("viewType"),
                "raw_meta":     vraw,
            })

        # If we couldn't find a "default" view, fall back to the first view,
        # or — for known standard forms — a guessed convention.
        if not default_view_name and views_out:
            default_view_name = views_out[0]["view_name"]
        if not default_view_name:
            # Common Zoho convention: <FormLinkName>View.
            default_view_name = f"{form_link}View"
            log.fine(f"  {form_link}: no views discovered — guessing {default_view_name!r}")

        forms_out.append({
            "form_link_name": form_link,
            "display_name":   raw.get("displayName") or raw.get("display_name"),
            "is_custom":      bool(raw.get("iscustom") or raw.get("isCustom")),
            "component_id":   raw.get("componentId") or raw.get("component_id"),
            "default_view":   default_view_name,
            "views":          views_out,
            "raw_meta":       raw,
            "table":          table_for_form(form_link),
        })

    _discovery_cache = {"forms": forms_out}
    log.info(f"Discovery complete: {len(forms_out)} form(s) → "
             f"{len(forms_out)} data table(s) (plus subform child tables, "
             f"created lazily on first record).")
    return _discovery_cache


# ── Static + dynamic schema list ────────────────────────────────────────────
def get_schema(configuration: dict) -> list:
    """Build the full table list. Subform child tables are NOT pre-declared
    — the SDK supports tables created on first upsert from data."""
    from helpers import validate_configuration, config_bool
    validate_configuration(configuration)

    schema_list = [
        # Meta tables
        {"table": "forms_meta",       "primary_key": ["form_link_name"]},
        {"table": "views_meta",       "primary_key": ["form_link_name", "view_name"]},
        {"table": "form_fields",      "primary_key": ["form_link_name", "field_label"]},

        # Files + categories
        {"table": "file_categories",  "primary_key": ["file_category_id"]},
        {"table": "files",            "primary_key": ["file_id"]},

        # Holidays + leave reference
        {"table": "holidays",         "primary_key": ["id"]},
        {"table": "leave_types",      "primary_key": ["id"]},

        # Transactional
        {"table": "attendance_daily", "primary_key": ["employee_id", "date"]},
        {"table": "leave_records",      "primary_key": ["id"]},
        {"table": "leave_records_days", "primary_key": ["leave_id", "date"]},
        {"table": "leave_balance",      "primary_key": ["employee_id", "leave_type_id",
                                                         "from_date", "to_date"]},
        {"table": "jobs",             "primary_key": ["job_id"]},
        {"table": "timelogs",         "primary_key": ["timelog_id"]},

        # Timetracker — clients, projects, timesheets-as-entity
        {"table": "timetracker_clients",  "primary_key": ["client_id"]},
        {"table": "timetracker_projects", "primary_key": ["project_id"]},
        {"table": "timesheets",           "primary_key": ["timesheet_id"]},

        # Attendance enrichment beyond daily summary
        {"table": "attendance_regularization", "primary_key": ["record_id"]},
        {"table": "shift_mappings",            "primary_key": ["employee_id",
                                                                "date"]},

        # HR ops + comms
        {"table": "cases",         "primary_key": ["case_id"]},
        {"table": "announcements", "primary_key": ["announcement_id"]},
    ]

    if config_bool(configuration, "sync_lms_courses"):
        schema_list.append({"table": "courses", "primary_key": ["course_id"]})
        schema_list.append({"table": "learner_progress",
                            "primary_key": ["learner_id", "course_id"]})

    if config_bool(configuration, "sync_attendance_entries"):
        schema_list.append({"table": "attendance_entries",
                            "primary_key": ["employee_id", "date"]})
        schema_list.append({"table": "attendance_entries_punches",
                            "primary_key": ["employee_id", "date", "punch_idx"]})

    if config_bool(configuration, "sync_attendance_latest_entries"):
        schema_list.append({"table": "attendance_latest_entries",
                            "primary_key": ["employee_id", "date", "entry_idx"]})

    # Dynamic per-form tables. Each has `record_id` as PK — that's the
    # field name Zoho People uses across both the records endpoint
    # (`recordId`) and the bulk endpoint (record map key). We normalise
    # both to `record_id` in the data-sync layer.
    catalog = discover(configuration)
    for form in catalog.get("forms", []):
        schema_list.append({
            "table":       form["table"],
            "primary_key": ["record_id"],
        })

    log.info(f"Schema: {len(schema_list)} tables declared "
             f"({len(schema_list) - len(catalog.get('forms', []))} static + "
             f"{len(catalog.get('forms', []))} per-form).")
    return schema_list
