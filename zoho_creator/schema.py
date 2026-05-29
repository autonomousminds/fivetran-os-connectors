"""
Schema generation for the Zoho Creator connector.

Zoho Creator's schema is dynamic — every customer has different apps, forms,
and reports. We discover the layout at sync start and return a Fivetran-
compatible table list.

## How discovery works

Zoho Creator's `/meta/applications` endpoint only returns apps the *authenticated
user owns directly* — it returns `{"code": 3520, "No apps available"}` when the
user has only Category-2 shared access. The workspace-scoped variant
`/meta/{workspace}/applications` does list every app in a given workspace
including Category-2 shares. We therefore iterate over `workspaces` from
config (a list of workspace names) and call the workspace-scoped endpoint
once per workspace.

## What we sync

5 fixed meta tables (always full-sync, columns explicit):
  - applications      one row per app
  - forms             one row per form
  - reports           one row per report
  - form_fields       one row per (form, field)
  - subform_fields    catalog of subform fields per form

Plus one dynamic data table **per report** (not per form — Zoho's v2.1 reports
endpoint does NOT return `base_form_link_name`, so we can't map reports to
forms automatically; reports are also the only data-readable surface). Table
name: `data_{workspace}__{app_link_name}__{report_link_name}`.

Subform child tables are created lazily — we don't pre-declare them. When a
record yields a subform array at sync time, `op.upsert` creates the child
table on first write. (Fivetran's SDK supports tables not declared in
`schema()` and infers columns from data.)
"""

import re

from fivetran_connector_sdk import Logging as log


_discovery_cache: dict = {}


def reset_discovery_cache():
    global _discovery_cache
    _discovery_cache = {}


_SAFE_RE = re.compile(r"[^a-z0-9]+")


def _safe(name: str) -> str:
    """Lowercase + replace runs of non-alphanumerics with single underscore.
    Zoho link_names use hyphens and underscores; warehouses prefer underscores.
    """
    return _SAFE_RE.sub("_", (name or "").lower()).strip("_")


def table_for_report(workspace: str, app_link: str, report_link: str) -> str:
    return f"data_{_safe(workspace)}__{_safe(app_link)}__{_safe(report_link)}"


def get_workspaces(configuration: dict) -> list:
    """Return list of workspace owner_names to scan. Accepts either
    `workspaces` (preferred — list) or `account_owner_name` (legacy — string).
    """
    ws = configuration.get("workspaces")
    if isinstance(ws, list) and ws:
        return ws
    single = configuration.get("account_owner_name")
    if single:
        return [single]
    raise ValueError(
        "Configuration must include either `workspaces` (list of workspace "
        "owner names) or `account_owner_name` (single workspace owner name)."
    )


def discover(configuration: dict) -> dict:
    """Walk every workspace → app → forms+reports → fields, return one cached
    catalog. Shape:

        {
            "workspaces": [
                {
                    "owner": "acme",
                    "apps": [
                        {
                            "link_name":    "acme-crm",
                            "display_name": "Acme CRM",
                            "category":     2,
                            "workspace":    "acme",
                            "raw_meta":     <original meta dict>,
                            "forms": [
                                {"link_name": "Client", "display_name": "Client",
                                 "field_link_names": [...]},
                                ...
                            ],
                            "reports": [
                                {"link_name": "Client_Report",
                                 "display_name": "Active Client Report",
                                 "type": 1,
                                 "table": "data_acme__acme_crm__client_report"},
                                ...
                            ],
                        },
                    ],
                },
                ...
            ],
        }
    """
    global _discovery_cache
    if _discovery_cache:
        return _discovery_cache

    from api_client import api_request
    from auth import api_host

    base = f"{api_host(configuration)}/creator/v2.1/meta"

    log.info("Discovering Zoho Creator schema (workspaces → apps → forms → reports → fields)...")

    workspaces_out = []
    for ws_owner in get_workspaces(configuration):
        ws_apps_resp = api_request(configuration, f"{base}/{ws_owner}/applications")
        ws_apps_raw = ws_apps_resp.get("applications") or []
        log.info(f"  /meta/{ws_owner}/applications → {len(ws_apps_raw)} apps")

        apps_out = []
        for a in ws_apps_raw:
            app_link = a.get("link_name")
            if not app_link:
                log.warning(f"  Skipping app with no link_name: {a}")
                continue

            forms_raw = (api_request(
                configuration, f"{base}/{ws_owner}/{app_link}/forms"
            ).get("forms") or [])

            reports_raw = (api_request(
                configuration, f"{base}/{ws_owner}/{app_link}/reports"
            ).get("reports") or [])

            forms_out = []
            for f in forms_raw:
                form_link = f.get("link_name")
                if not form_link:
                    continue
                try:
                    fields_resp = api_request(
                        configuration,
                        f"{base}/{ws_owner}/{app_link}/form/{form_link}/fields",
                    )
                    fields_raw = fields_resp.get("fields") or []
                except Exception as e:
                    log.warning(f"  fields fetch failed for {app_link}/{form_link}: {e!r}")
                    fields_raw = []
                forms_out.append({
                    "link_name":         form_link,
                    "display_name":      f.get("display_name"),
                    "type":              f.get("type"),
                    "field_link_names":  [
                        fd.get("link_name") or fd.get("field_link_name")
                        for fd in fields_raw
                        if fd.get("link_name") or fd.get("field_link_name")
                    ],
                    "raw_fields":        fields_raw,
                    "raw_meta":          f,
                })

            reports_out = []
            for r in reports_raw:
                rlink = r.get("link_name")
                if not rlink:
                    continue
                reports_out.append({
                    "link_name":    rlink,
                    "display_name": r.get("display_name"),
                    "type":         r.get("type"),
                    "table":        table_for_report(ws_owner, app_link, rlink),
                    "raw_meta":     r,
                })

            apps_out.append({
                "link_name":    app_link,
                "display_name": a.get("application_name"),
                "category":     a.get("category"),
                "workspace":    ws_owner,
                "raw_meta":     a,
                "forms":        forms_out,
                "reports":      reports_out,
            })

        workspaces_out.append({"owner": ws_owner, "apps": apps_out})

    _discovery_cache = {"workspaces": workspaces_out}
    n_apps = sum(len(w["apps"]) for w in workspaces_out)
    n_forms = sum(len(a["forms"]) for w in workspaces_out for a in w["apps"])
    n_reports = sum(len(a["reports"]) for w in workspaces_out for a in w["apps"])
    log.info(
        f"Discovery complete: {len(workspaces_out)} workspace(s), {n_apps} apps, "
        f"{n_forms} forms, {n_reports} reports (= data tables)."
    )
    return _discovery_cache


def get_schema(configuration: dict) -> list:
    """Build the table list Fivetran creates upfront.

    Subform child tables are NOT pre-declared — they're created on first
    upsert from real data at sync time. The Connector SDK supports that.
    """
    schema_list = [
        {"table": "applications",   "primary_key": ["workspace", "link_name"]},
        {"table": "forms",          "primary_key": ["workspace", "app_link_name", "link_name"]},
        {"table": "reports",        "primary_key": ["workspace", "app_link_name", "link_name"]},
        {"table": "form_fields",    "primary_key": ["workspace", "app_link_name", "form_link_name", "link_name"]},
        {"table": "subform_fields", "primary_key": ["workspace", "app_link_name", "form_link_name", "link_name"]},
    ]

    catalog = discover(configuration)
    for ws in catalog["workspaces"]:
        for app in ws["apps"]:
            for report in app["reports"]:
                schema_list.append({
                    "table":       report["table"],
                    "primary_key": ["ID"],
                })

    log.info(f"Schema: {len(schema_list)} tables declared (5 meta + {len(schema_list) - 5} data).")
    return schema_list
