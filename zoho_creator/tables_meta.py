"""
Meta-table sync for the Zoho Creator connector.

These tables describe the Creator account itself; they're always fully
re-synced because they're small and downstream data syncs depend on them
being current within the same run.

Tables produced:
  - applications      one row per app (per workspace)
  - forms             one row per form
  - reports           one row per report
  - form_fields       one row per (form, field)
  - subform_fields    one row per (form, subform_field) — catalog only;
                       actual subform data lands in dynamic child tables at
                       record-sync time
"""

import json

from fivetran_connector_sdk import Logging as log

from helpers import upsert
from schema import discover


# Zoho field-type codes that represent a subform / nested form.
# Type 19 is the canonical "Subform" in v2.1. Some response shapes also
# report "subform" as a string in `type`/`field_type`; we accept either.
_SUBFORM_TYPE_CODES = {19}


def _is_subform_field(field_meta: dict) -> bool:
    t = field_meta.get("type") or field_meta.get("field_type")
    if isinstance(t, int):
        return t in _SUBFORM_TYPE_CODES
    if isinstance(t, str):
        return "subform" in t.lower()
    return False


def sync_meta_all(configuration: dict, state: dict):
    catalog = discover(configuration)
    n_apps = n_forms = n_reports = n_fields = n_subs = 0

    for ws in catalog["workspaces"]:
        owner = ws["owner"]
        for app in ws["apps"]:
            app_link = app["link_name"]

            upsert("applications", {
                "workspace":    owner,
                "link_name":    app_link,
                "display_name": app.get("display_name"),
                "category":     app.get("category"),
                "raw_meta":     json.dumps(app.get("raw_meta") or {}),
            })
            n_apps += 1

            for form in app.get("forms") or []:
                upsert("forms", {
                    "workspace":     owner,
                    "app_link_name": app_link,
                    "link_name":     form["link_name"],
                    "display_name":  form.get("display_name"),
                    "type":          form.get("type"),
                    "raw_meta":      json.dumps(form.get("raw_meta") or {}),
                })
                n_forms += 1

                for fd in form.get("raw_fields") or []:
                    fl = fd.get("link_name") or fd.get("field_link_name")
                    if not fl:
                        continue
                    upsert("form_fields", {
                        "workspace":      owner,
                        "app_link_name":  app_link,
                        "form_link_name": form["link_name"],
                        "link_name":      fl,
                        "display_name":   fd.get("display_name"),
                        "type":           fd.get("type") or fd.get("field_type"),
                        "raw_meta":       json.dumps(fd),
                    })
                    n_fields += 1
                    if _is_subform_field(fd):
                        upsert("subform_fields", {
                            "workspace":      owner,
                            "app_link_name":  app_link,
                            "form_link_name": form["link_name"],
                            "link_name":      fl,
                            "display_name":   fd.get("display_name"),
                        })
                        n_subs += 1

            for r in app.get("reports") or []:
                upsert("reports", {
                    "workspace":     owner,
                    "app_link_name": app_link,
                    "link_name":     r["link_name"],
                    "display_name":  r.get("display_name"),
                    "type":          r.get("type"),
                    "table":         r.get("table"),
                    "raw_meta":      json.dumps(r.get("raw_meta") or {}),
                })
                n_reports += 1

    log.info(
        f"meta synced: applications={n_apps}, forms={n_forms}, "
        f"reports={n_reports}, form_fields={n_fields}, subform_fields={n_subs}"
    )
