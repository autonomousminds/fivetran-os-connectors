"""
Portal-level metadata sync for the Zoho Projects connector.

Every function here is full-sync each run — these tables are small, change
infrequently, and downstream queries assume the snapshot is current.

Public entry points (each takes `configuration, state, portal_id` unless
noted):
  - `sync_portals(configuration, state)`           — discovers portals, returns
                                                     list of portal_ids
  - `sync_modules(...)`                            — V3 modules metadata
  - `sync_fields(...)`                             — V3 fields metadata per module
  - `sync_project_layouts(...)`
  - `sync_task_layouts(...)`
  - `sync_timesheet_layouts(...)`                  — iterates projects under the hood
  - `sync_project_custom_fields_meta(...)`
  - `sync_task_custom_fields_meta(...)`
  - `sync_bug_custom_fields_meta(...)`             — iterates projects
  - `sync_timesheet_custom_fields_meta(...)`
  - `sync_project_custom_statuses(...)`
  - `sync_bug_default_fields(...)`                 — iterates projects
  - `sync_bug_renamed_fields(...)`                 — iterates projects
  - `sync_project_groups(...)`
  - `sync_tags(...)`

`bug_*_fields` and `timesheet_layouts` are per-project endpoints (not
portal-wide) — they iterate `_list_active_project_ids(portal_id, state)`
which reads the projects we've already cached during the data-sync pass.
For first-run fallback, they re-list projects from the API.
"""

import json

from fivetran_connector_sdk import Logging as log

from api_client import (
    ScopeMissing,
    api_request,
    build_url,
    paginate_v2,
    paginate_v3,
)
from helpers import (
    config_list,
    flatten_record,
    ids_seen,
    reconcile_deletes,
    upsert,
)


def _extract_list(body, *keys):
    """Pull the first NON-EMPTY list value out of a Zoho Projects response.

    Tries each key in `keys` (in order); falls back to a bare-list body
    and to the conventional fallback keys (`data`, `result`). Skipping
    empty matches matters because several Zoho endpoints return shapes
    like `{"default_fields": [], "renamed_fields": [...]}` where the
    first key is a placeholder and the actual data lives under the
    second. If no non-empty list is found, returns the first empty list
    we saw (or `[]`).
    """
    if isinstance(body, list):
        return body
    if not isinstance(body, dict):
        return []
    first_empty = None
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            if v:
                return v
            if first_empty is None:
                first_empty = v
    for k in ("data", "result"):
        v = body.get(k)
        if isinstance(v, list):
            if v:
                return v
            if first_empty is None:
                first_empty = v
    return first_empty if first_empty is not None else []


def _extract_one(body, *keys):
    """Pull a single nested dict out of a Zoho Projects response (e.g. for
    detail endpoints that wrap the object in a single-key list)."""
    if isinstance(body, dict):
        for k in keys:
            v = body.get(k)
            if isinstance(v, dict):
                return v
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v[0]
        return body
    if isinstance(body, list) and body and isinstance(body[0], dict):
        return body[0]
    return {}


# ═══════════════════════════════════════════════════════════════════════════
#  Portals (entry point — figures out which portals to sync)
# ═══════════════════════════════════════════════════════════════════════════
def sync_portals(configuration: dict, state: dict) -> list:
    """List portals available to the OAuth grant.

    Filters by `portal_ids` in configuration when non-empty. Returns the
    list of portal IDs (as strings) the rest of the sync will iterate.
    Writes the `portals` table.
    """
    url = build_url(configuration, None, "/restapi/portals/")
    try:
        body = api_request(configuration, url)
    except ScopeMissing as e:
        log.severe(f"portals: scope missing — {e!s}. Aborting (need at least "
                   f"ZohoProjects.portals.READ).")
        return []

    raw = _extract_list(body, "portals", "login_info")
    if not raw:
        log.warning("portals: /restapi/portals/ returned no portals — "
                    "is the refresh_token correct?")
        return []

    configured = set(config_list(configuration, "portal_ids"))

    portal_ids: list = []
    for p in raw:
        if not isinstance(p, dict):
            continue
        pid = p.get("id") or p.get("id_string") or p.get("portal_id")
        if pid is None:
            continue
        pid = str(pid)
        if configured and pid not in configured:
            log.fine(f"portals: skipping {pid} (not in portal_ids filter)")
            continue

        flat = flatten_record(p)
        flat["portal_id"] = pid
        upsert("portals", flat, id_key="portal_id")
        portal_ids.append(pid)

    log.info(f"portals: {len(portal_ids)} portal(s) selected for sync "
             f"(of {len(raw)} returned).")
    reconcile_deletes("portals", ids_seen("portals"), state,
                      key_template="portal_id")
    return portal_ids


# ═══════════════════════════════════════════════════════════════════════════
#  V3 modules + fields metadata (portal-wide)
# ═══════════════════════════════════════════════════════════════════════════
def sync_modules(configuration: dict, state: dict, portal_id: str):
    """V3 modules metadata. Returns the list of module api names so the
    caller can drive `sync_fields`."""
    url = build_url(configuration, portal_id, "/modules/", version="v3")
    try:
        body = api_request(configuration, url)
    except ScopeMissing as e:
        log.warning(f"modules ({portal_id}): scope missing — {e!s}")
        return []
    except Exception as e:
        log.warning(f"modules ({portal_id}): {e!r}")
        return []

    raw = _extract_list(body, "modules", "module")
    module_api_names: list = []
    for m in raw:
        if not isinstance(m, dict):
            continue
        api_name = m.get("api_name") or m.get("module_api_name") or m.get("name")
        if not api_name:
            continue
        module_api_names.append(api_name)

        flat = flatten_record(m)
        flat["portal_id"] = portal_id
        flat["module_api_name"] = api_name
        upsert("modules", flat, id_key="portal_id|module_api_name")

    log.info(f"modules ({portal_id}): {len(module_api_names)} module(s)")
    return module_api_names


def sync_fields(configuration: dict, state: dict, portal_id: str,
                module_api_names: list):
    """V3 fields metadata. One row per (portal, module, field)."""
    n_fields = 0
    for module_api_name in (module_api_names or []):
        url = build_url(
            configuration, portal_id,
            f"/modules/{module_api_name}/fields/", version="v3",
        )
        try:
            body = api_request(configuration, url)
        except ScopeMissing:
            continue
        except Exception as e:
            log.fine(f"fields ({portal_id}/{module_api_name}): {e!r}")
            continue

        raw = _extract_list(body, "fields", "field")
        for f in raw:
            if not isinstance(f, dict):
                continue
            field_api_name = (f.get("api_name") or f.get("field_api_name")
                              or f.get("field_name"))
            if not field_api_name:
                continue
            flat = flatten_record(f)
            flat["portal_id"] = portal_id
            flat["module_api_name"] = module_api_name
            flat["field_api_name"] = field_api_name
            upsert("fields", flat,
                   id_key="portal_id|module_api_name|field_api_name")
            n_fields += 1

    log.info(f"fields ({portal_id}): {n_fields} field(s) across "
             f"{len(module_api_names)} module(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  Layouts (portal-wide + per-project for timesheets)
# ═══════════════════════════════════════════════════════════════════════════
def sync_project_layouts(configuration: dict, state: dict, portal_id: str):
    url = build_url(configuration, portal_id, "/module/projects/layouts/")
    try:
        body = api_request(configuration, url)
    except ScopeMissing as e:
        log.warning(f"project_layouts ({portal_id}): scope missing — {e!s}")
        return
    except Exception as e:
        log.warning(f"project_layouts ({portal_id}): {e!r}")
        return

    raw = _extract_list(body, "layouts", "module_layouts", "result")
    for layout in raw:
        if not isinstance(layout, dict):
            continue
        lid = layout.get("id") or layout.get("layout_id")
        if lid is None:
            continue
        flat = flatten_record(layout)
        flat["portal_id"] = portal_id
        flat["layout_id"] = str(lid)
        upsert("project_layouts", flat, id_key="portal_id|layout_id")
    log.info(f"project_layouts ({portal_id}): {len(raw)} layout(s)")


def sync_task_layouts(configuration: dict, state: dict, portal_id: str):
    url = build_url(configuration, portal_id, "/tasklayouts")
    try:
        body = api_request(configuration, url)
    except ScopeMissing as e:
        log.warning(f"task_layouts ({portal_id}): scope missing — {e!s}")
        return
    except Exception as e:
        log.warning(f"task_layouts ({portal_id}): {e!r}")
        return

    raw = _extract_list(body, "tasklayouts", "task_layouts", "layouts")
    for layout in raw:
        if not isinstance(layout, dict):
            continue
        lid = layout.get("id") or layout.get("layout_id")
        if lid is None:
            continue
        flat = flatten_record(layout)
        flat["portal_id"] = portal_id
        flat["layout_id"] = str(lid)
        upsert("task_layouts", flat, id_key="portal_id|layout_id")
    log.info(f"task_layouts ({portal_id}): {len(raw)} layout(s)")


def sync_timesheet_layouts(configuration: dict, state: dict, portal_id: str,
                           project_ids: list):
    """Per-project endpoint — iterate the projects we've discovered."""
    n_ok = 0
    for project_id in (project_ids or []):
        url = build_url(configuration, portal_id,
                        f"/projects/{project_id}/timesheetlayouts")
        try:
            body = api_request(configuration, url)
        except ScopeMissing:
            continue
        except Exception as e:
            log.fine(f"timesheet_layouts ({portal_id}/{project_id}): {e!r}")
            continue

        layout = _extract_one(body, "timesheetlayout", "layout")
        if not layout:
            continue
        flat = flatten_record(layout)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        upsert("timesheet_layouts", flat, id_key="portal_id|project_id")
        n_ok += 1
    log.info(f"timesheet_layouts ({portal_id}): {n_ok} project(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  Custom-field metadata (portal-wide + per-project for bugs)
# ═══════════════════════════════════════════════════════════════════════════
def _sync_custom_fields_meta(configuration: dict, table: str, portal_id: str,
                             url: str, extra_cols: dict = None,
                             id_key: str = "portal_id|field_api_name"):
    """Shared writer used by the portal-wide custom-field metadata syncs.
    Bug custom fields use a different shape (`extra_cols` adds project_id)."""
    try:
        body = api_request(configuration, url)
    except ScopeMissing as e:
        log.warning(f"{table} ({portal_id}): scope missing — {e!s}")
        return
    except Exception as e:
        log.warning(f"{table} ({portal_id}): {e!r}")
        return

    # Zoho returns custom-field metadata under any of these keys depending
    # on the parent resource and the API surface (V2 vs V3): the snake_case
    # form (`project_custom_fields`, `task_custom_fields`,
    # `bug_custom_fields`, `timesheet_custom_fields`) is the most common
    # but the generic `customfields` and pluralised `fields` also appear.
    raw = _extract_list(body, "project_custom_fields", "task_custom_fields",
                        "bug_custom_fields", "timesheet_custom_fields",
                        "customfields", "custom_fields", "fields")
    for f in raw:
        if not isinstance(f, dict):
            continue
        api_name = (f.get("column_name") or f.get("api_name")
                    or f.get("field_api_name") or f.get("UDF") or f.get("udf"))
        if not api_name:
            # Some shapes nest the api name under `id`.
            api_name = f.get("id") or f.get("field_id")
        if not api_name:
            continue
        flat = flatten_record(f)
        flat["portal_id"] = portal_id
        if extra_cols:
            flat.update(extra_cols)
        flat["field_api_name"] = str(api_name)
        # Surface label + type onto consistent columns so the join into the
        # parent custom-field child tables is straightforward.
        flat.setdefault("field_label",
                        f.get("label_name") or f.get("label")
                        or f.get("display_label") or f.get("field_name"))
        flat.setdefault("field_type",
                        f.get("field_type") or f.get("type"))
        upsert(table, flat, id_key=id_key)
    log.fine(f"{table} ({portal_id}{('/' + str(extra_cols['project_id'])) if extra_cols else ''}): "
             f"{len(raw)} custom field(s)")


def sync_project_custom_fields_meta(configuration, state, portal_id):
    _sync_custom_fields_meta(
        configuration, "project_custom_fields_meta", portal_id,
        build_url(configuration, portal_id, "/projects/customfields/"))


def sync_task_custom_fields_meta(configuration, state, portal_id):
    # Task custom fields don't have a dedicated `/tasks/customfields/`
    # endpoint at portal level — they're discovered via the V3 Fields
    # metadata. We still try the legacy `/tasks/customfields/` first
    # because some portals expose it; on 404/scope error we fall back
    # silently to the V3 metadata that lives in `fields` table.
    url = build_url(configuration, portal_id, "/tasks/customfields/")
    try:
        body = api_request(configuration, url)
    except ScopeMissing:
        return
    except Exception:
        return
    raw = _extract_list(body, "task_custom_fields", "customfields",
                        "custom_fields", "fields")
    if not raw:
        return
    for f in raw:
        if not isinstance(f, dict):
            continue
        api_name = (f.get("column_name") or f.get("api_name")
                    or f.get("field_api_name"))
        if not api_name:
            continue
        flat = flatten_record(f)
        flat["portal_id"] = portal_id
        flat["field_api_name"] = str(api_name)
        flat.setdefault("field_label",
                        f.get("label_name") or f.get("label")
                        or f.get("field_name"))
        flat.setdefault("field_type",
                        f.get("field_type") or f.get("type"))
        upsert("task_custom_fields_meta", flat,
               id_key="portal_id|field_api_name")
    log.fine(f"task_custom_fields_meta ({portal_id}): {len(raw)} field(s)")


def sync_bug_custom_fields_meta(configuration, state, portal_id, project_ids):
    for project_id in (project_ids or []):
        _sync_custom_fields_meta(
            configuration, "bug_custom_fields_meta", portal_id,
            build_url(configuration, portal_id,
                      f"/projects/{project_id}/bugs/customfields/"),
            extra_cols={"project_id": str(project_id)},
            id_key="portal_id|project_id|field_api_name",
        )


def sync_timesheet_custom_fields_meta(configuration, state, portal_id):
    _sync_custom_fields_meta(
        configuration, "timesheet_custom_fields_meta", portal_id,
        build_url(configuration, portal_id, "/timesheetcustomfields"))


# ═══════════════════════════════════════════════════════════════════════════
#  Project custom statuses
# ═══════════════════════════════════════════════════════════════════════════
def sync_project_custom_statuses(configuration, state, portal_id):
    url = build_url(configuration, portal_id, "/module/projects/status/")
    try:
        body = api_request(configuration, url)
    except ScopeMissing as e:
        log.warning(f"project_custom_statuses ({portal_id}): scope missing — {e!s}")
        return
    except Exception as e:
        log.warning(f"project_custom_statuses ({portal_id}): {e!r}")
        return

    raw = _extract_list(body, "project_custom_status", "statuses", "status",
                        "result")
    for s in raw:
        if not isinstance(s, dict):
            continue
        sid = s.get("id") or s.get("status_id")
        if sid is None:
            continue
        flat = flatten_record(s)
        flat["portal_id"] = portal_id
        flat["status_id"] = str(sid)
        upsert("project_custom_statuses", flat,
               id_key="portal_id|status_id")
    log.info(f"project_custom_statuses ({portal_id}): {len(raw)} status(es)")


# ═══════════════════════════════════════════════════════════════════════════
#  Bug default + renamed fields (per-project)
# ═══════════════════════════════════════════════════════════════════════════
def _sync_bug_field_table(configuration, portal_id, project_ids,
                          table: str, path_suffix: str,
                          preferred_keys: tuple = ()):
    """Sync a bug-field metadata table (defaults or renamed).

    `preferred_keys` is the ORDERED list of response keys to try first —
    this matters because Zoho's `renamedfields` endpoint returns BOTH
    `default_fields` and `renamed_fields` keys in the response (with
    only the requested one populated for the rename endpoint, but both
    populated for some portal configs). Without endpoint-specific
    extraction we'd pick the wrong key.
    """
    for project_id in (project_ids or []):
        url = build_url(configuration, portal_id,
                        f"/projects/{project_id}/bugs/{path_suffix}")
        try:
            body = api_request(configuration, url)
        except ScopeMissing:
            continue
        except Exception as e:
            log.fine(f"{table} ({portal_id}/{project_id}): {e!r}")
            continue

        # Try the endpoint-preferred keys first, then the generic fallbacks.
        all_keys = list(preferred_keys) + [
            "default_fields", "renamed_fields", "fields",
            "defaultfields", "renamedfields", "result",
        ]
        raw = _extract_list(body, *all_keys)
        if not raw and isinstance(body, dict):
            # Some endpoints return a flat dict keyed by field_name.
            raw = [dict(field_name=k, **(v if isinstance(v, dict) else {"value": v}))
                   for k, v in body.items()
                   if k not in ("status", "errorcode", "error", "message")]

        for f in raw:
            if not isinstance(f, dict):
                continue
            field_name = (f.get("field_name") or f.get("fieldName")
                          or f.get("name"))
            if not field_name:
                continue
            flat = flatten_record(f)
            flat["portal_id"] = portal_id
            flat["project_id"] = str(project_id)
            flat["field_name"] = field_name
            upsert(table, flat,
                   id_key="portal_id|project_id|field_name")


def sync_bug_default_fields(configuration, state, portal_id, project_ids):
    _sync_bug_field_table(
        configuration, portal_id, project_ids,
        "bug_default_fields", "defaultfields/",
        preferred_keys=("defaultfields", "default_fields"),
    )
    log.info(f"bug_default_fields ({portal_id}): processed "
             f"{len(project_ids or [])} project(s)")


def sync_bug_renamed_fields(configuration, state, portal_id, project_ids):
    _sync_bug_field_table(
        configuration, portal_id, project_ids,
        "bug_renamed_fields", "renamedfields/",
        preferred_keys=("renamedfields", "renamed_fields"),
    )
    log.info(f"bug_renamed_fields ({portal_id}): processed "
             f"{len(project_ids or [])} project(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  Project groups
# ═══════════════════════════════════════════════════════════════════════════
def sync_project_groups(configuration, state, portal_id):
    url = build_url(configuration, portal_id, "/projects/groups")
    try:
        records = list(paginate_v2(configuration, url, "groups", page_size=200))
    except ScopeMissing as e:
        log.warning(f"project_groups ({portal_id}): scope missing — {e!s}")
        return
    except Exception as e:
        log.warning(f"project_groups ({portal_id}): {e!r}")
        return

    for g in records:
        if not isinstance(g, dict):
            continue
        gid = g.get("id") or g.get("group_id")
        if gid is None:
            continue
        flat = flatten_record(g)
        flat["portal_id"] = portal_id
        flat["group_id"] = str(gid)
        upsert("project_groups", flat, id_key="portal_id|group_id")
    log.info(f"project_groups ({portal_id}): {len(records)} group(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  Tags (V3)
# ═══════════════════════════════════════════════════════════════════════════
def sync_tags(configuration, state, portal_id):
    url = build_url(configuration, portal_id, "/tags", version="v3")
    try:
        records = list(paginate_v3(configuration, url, "tags", per_page=200))
    except ScopeMissing as e:
        log.warning(f"tags ({portal_id}): scope missing — {e!s}")
        return
    except Exception as e:
        log.warning(f"tags ({portal_id}): {e!r}")
        return

    for t in records:
        if not isinstance(t, dict):
            continue
        tid = t.get("id") or t.get("tag_id") or t.get("id_string")
        if tid is None:
            continue
        flat = flatten_record(t)
        flat["portal_id"] = portal_id
        flat["tag_id"] = str(tid)
        upsert("tags", flat, id_key="portal_id|tag_id")
    log.info(f"tags ({portal_id}): {len(records)} tag(s)")
