"""
Transactional / high-volume tables for the Zoho Projects connector.

Six logical groups (all called from `connector.update()`, per portal):

  1. **People & clients** — portal-wide tables: users, clients, client_users,
     leaves (V3). All full-sync per run; volumes are low.

  2. **Time logs** — portal-wide `/logs` with `fetch_by_modifiedtime`.
     Single union table with `component_type` column ∈ {`task`, `bug`,
     `general`}.

  3. **Projects + per-project fan-out** — projects in three status passes
     (active / archived / template), LMT-filtered. Then for each project:
     milestones, tasklists, tasks, bugs, events, forums, documents,
     activities feed.

  4. **Tasks tree** — for each task: subtasks, comments (LMT), attachments,
     followers, dependencies (extracted from Task Details), activities.
     Plus portal-wide `task_status_history` (V3, LMT).

  5. **Bugs tree** — full refresh per project. For each bug: comments,
     attachments, resolution, timer, followers, activities. Plus
     bug-task associations (V3, per bug).

  6. **Tag associations** — junction table assembled as a side-effect of
     the project/task/bug/milestone/forum syncs by inspecting embedded
     tag arrays on each record. Written incrementally during sync.

## Custom-field metadata cache

UDF metadata (label, type) lives in the `*_custom_fields_meta` tables
written by `tables_meta.py`. To avoid coupling, this module loads UDF
metadata lazily on first use, scoped by `(portal_id, scope[, project_id])`.
"""

import json
import time

from fivetran_connector_sdk import Logging as log

from api_client import (
    DailyLimitExceeded,
    ScopeMissing,
    api_request,
    build_url,
    paginate_v2,
    paginate_v3,
)
from helpers import (
    build_udf_row,
    config_bool,
    extract_subforms,
    extract_udfs,
    flatten_record,
    ids_seen,
    reconcile_deletes,
    upsert,
)


CHECKPOINT_EVERY = 500


# ── UDF metadata cache (loaded lazily per scope) ────────────────────────────
# Key: (portal_id, scope, project_id_or_None) → {api_name: {field_label, field_type}}
_udf_meta_cache: dict = {}


def reset_udf_meta_cache():
    _udf_meta_cache.clear()


def _udf_meta(configuration: dict, portal_id: str, scope: str,
              project_id: str = None) -> dict:
    """Look up custom-field metadata for a (portal, scope[, project]) tuple.
    Scope ∈ {'projects', 'tasks', 'bugs', 'timesheets'}.

    Lazily fetches from the corresponding `*customfields` endpoint and caches
    the result. Returns an empty dict on scope-error / 404 / unsupported.
    """
    cache_key = (portal_id, scope, project_id)
    if cache_key in _udf_meta_cache:
        return _udf_meta_cache[cache_key]

    paths = {
        "projects":   "/projects/customfields/",
        "tasks":      "/tasks/customfields/",
        "bugs":       f"/projects/{project_id}/bugs/customfields/"
                       if project_id else None,
        "timesheets": "/timesheetcustomfields",
    }
    path = paths.get(scope)
    if not path:
        _udf_meta_cache[cache_key] = {}
        return {}

    url = build_url(configuration, portal_id, path)
    try:
        body = api_request(configuration, url)
    except ScopeMissing:
        _udf_meta_cache[cache_key] = {}
        return {}
    except Exception:
        _udf_meta_cache[cache_key] = {}
        return {}

    raw = []
    if isinstance(body, dict):
        for k in ("customfields", "custom_fields", "fields"):
            v = body.get(k)
            if isinstance(v, list):
                raw = v
                break
    elif isinstance(body, list):
        raw = body

    meta = {}
    for f in raw:
        if not isinstance(f, dict):
            continue
        api_name = (f.get("column_name") or f.get("api_name")
                    or f.get("field_api_name") or f.get("UDF") or f.get("udf"))
        if not api_name:
            continue
        meta[str(api_name)] = {
            "field_label": (f.get("label_name") or f.get("label")
                            or f.get("field_name") or f.get("display_label")),
            "field_type":  f.get("field_type") or f.get("type"),
        }
    _udf_meta_cache[cache_key] = meta
    return meta


def _emit_udfs(child_table: str, record: dict, parent_keys: dict,
               meta: dict):
    """Pull UDF_<TYPE><N> keys out of `record` and write them to
    `child_table` as long-form rows. `parent_keys` is the dict that
    identifies the parent (e.g. `{"portal_id": ..., "task_id": ...}`)."""
    udfs = extract_udfs(record)
    if not udfs:
        return
    parent_pk_cols = "|".join(parent_keys.keys())
    id_key = f"{parent_pk_cols}|field_api_name"
    for api_name, value in udfs:
        row = build_udf_row(parent_keys, api_name, value, meta_by_api_name=meta)
        upsert(child_table, row, id_key=id_key)


# ── Tag associations (side-effect of every parent sync) ────────────────────
ENTITY_TYPE_CODES = {
    "project":   "2",
    "milestone": "3",
    "tasklist":  "4",
    "task":      "5",
    "bug":       "6",
    "forum":     "7",
    "status":    "8",
}


def _emit_tag_associations(portal_id: str, entity_type: str,
                            entity_id, record: dict):
    """Inspect a parent record for embedded tag data and write rows to
    `tag_associations`.

    Zoho Projects embeds tags on tagged objects in one of these shapes:
      - `"tags": [{"id": "...", "name": "...", "color": "..."}, ...]`
      - `"tags": [{"tag_id": "...", "tag_name": "..."}, ...]`
      - `"tag_ids": ["123", "456"]`  (rare; v3 some endpoints)

    Entity type code mapping is documented at
    https://www.zoho.com/projects/help/rest-api/tags.html.
    """
    if entity_id is None:
        return
    tag_array = record.get("tags") if isinstance(record, dict) else None
    tag_ids = record.get("tag_ids") if isinstance(record, dict) else None
    entity_code = ENTITY_TYPE_CODES.get(entity_type)
    if not entity_code:
        return

    pairs = []
    if isinstance(tag_array, list):
        for t in tag_array:
            if isinstance(t, dict):
                tid = t.get("id") or t.get("tag_id") or t.get("id_string")
                if tid is not None:
                    pairs.append((str(tid), t))
    if isinstance(tag_ids, list):
        for tid in tag_ids:
            if tid not in (None, ""):
                pairs.append((str(tid), {}))

    for tag_id, raw in pairs:
        row = {
            "portal_id":   portal_id,
            "entity_type": entity_type,
            "entity_id":   str(entity_id),
            "tag_id":      tag_id,
            "tag_name":    raw.get("name") or raw.get("tag_name"),
            "tag_color":   raw.get("color") or raw.get("colour"),
        }
        upsert("tag_associations", row,
               id_key="portal_id|entity_type|entity_id|tag_id")


# ── Generic emit helper ─────────────────────────────────────────────────────
def _emit_record(table: str, record: dict, portal_id: str, id_col: str,
                 record_id, extra_cols: dict = None,
                 id_key: str = None,
                 udf_child_table: str = None,
                 udf_meta: dict = None,
                 udf_parent_keys: dict = None,
                 entity_type_for_tags: str = None,
                 drop_keys: set = None):
    """One-shot writer: flatten the parent, strip UDFs, emit the parent row,
    then emit the UDF child rows and tag association rows.

    Returns the resolved `record_id` (string) so the caller can use it as
    a parent key for downstream fan-out (comments, attachments, etc.).
    """
    if record_id is None:
        return None
    flat = flatten_record(record, drop_keys=drop_keys or set())
    flat["portal_id"] = portal_id
    flat[id_col] = str(record_id)
    if extra_cols:
        flat.update(extra_cols)
    upsert(table, flat, id_key=id_key or f"portal_id|{id_col}")

    if udf_child_table:
        meta = udf_meta or {}
        parent_keys = udf_parent_keys or {"portal_id": portal_id,
                                          id_col: str(record_id)}
        _emit_udfs(udf_child_table, record, parent_keys, meta)

    if entity_type_for_tags:
        _emit_tag_associations(portal_id, entity_type_for_tags,
                                record_id, record)
    return str(record_id)


def _checkpoint_periodically(state: dict, counter: list):
    counter[0] += 1
    if counter[0] >= CHECKPOINT_EVERY:
        from fivetran_connector_sdk import Operations as op
        op.checkpoint(state=state)
        counter[0] = 0


# ═══════════════════════════════════════════════════════════════════════════
#  1. People & clients (portal-wide, full refresh)
# ═══════════════════════════════════════════════════════════════════════════
def sync_users(configuration, state, portal_id):
    url = build_url(configuration, portal_id, "/users/")
    try:
        body = api_request(configuration, url)
    except ScopeMissing as e:
        log.warning(f"users ({portal_id}): scope missing — {e!s}")
        return
    except Exception as e:
        log.warning(f"users ({portal_id}): {e!r}")
        return

    raw = body.get("users") if isinstance(body, dict) else (body or [])
    if not isinstance(raw, list):
        raw = []
    for u in raw:
        if not isinstance(u, dict):
            continue
        uid = u.get("id") or u.get("user_id") or u.get("zpuid")
        if uid is None:
            continue
        flat = flatten_record(u)
        flat["portal_id"] = portal_id
        flat["user_id"] = str(uid)
        upsert("users", flat, id_key="portal_id|user_id")
    log.info(f"users ({portal_id}): {len(raw)} user(s)")
    reconcile_deletes("users", ids_seen("users"), state,
                      key_template={"portal_id": 0, "user_id": 1},
                      state_key_suffix=f"__{portal_id}")


def sync_clients(configuration, state, portal_id):
    url = build_url(configuration, portal_id, "/clients/")
    try:
        body = api_request(configuration, url)
    except ScopeMissing as e:
        log.warning(f"clients ({portal_id}): scope missing — {e!s}")
        return []
    except Exception as e:
        log.warning(f"clients ({portal_id}): {e!r}")
        return []

    raw = body.get("clients") if isinstance(body, dict) else (body or [])
    if not isinstance(raw, list):
        raw = []
    client_ids: list = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        cid = c.get("id") or c.get("client_id")
        if cid is None:
            continue
        cid = str(cid)
        client_ids.append(cid)
        flat = flatten_record(c)
        flat["portal_id"] = portal_id
        flat["client_id"] = cid
        upsert("clients", flat, id_key="portal_id|client_id")
    log.info(f"clients ({portal_id}): {len(raw)} client(s)")
    reconcile_deletes("clients", ids_seen("clients"), state,
                      key_template={"portal_id": 0, "client_id": 1},
                      state_key_suffix=f"__{portal_id}")
    return client_ids


def sync_client_users(configuration, state, portal_id, client_ids):
    """Client users are surfaced via the per-client details endpoint —
    Zoho returns the user list nested under `users` in
    `/clients/{client_id}/`. There is no portal-wide client users list."""
    n = 0
    for client_id in (client_ids or []):
        url = build_url(configuration, portal_id, f"/clients/{client_id}/")
        try:
            body = api_request(configuration, url)
        except ScopeMissing:
            continue
        except Exception as e:
            log.fine(f"client_users ({portal_id}/{client_id}): {e!r}")
            continue

        users = []
        if isinstance(body, dict):
            inner = body.get("clients")
            if isinstance(inner, list) and inner:
                first = inner[0]
                if isinstance(first, dict):
                    users = first.get("users") or first.get("client_users") or []
            elif isinstance(inner, dict):
                users = inner.get("users") or []
            else:
                users = body.get("users") or body.get("client_users") or []
        if not isinstance(users, list):
            users = []

        for u in users:
            if not isinstance(u, dict):
                continue
            uid = u.get("id") or u.get("client_user_id") or u.get("zpuid")
            if uid is None:
                continue
            flat = flatten_record(u)
            flat["portal_id"] = portal_id
            flat["client_id"] = str(client_id)
            flat["client_user_id"] = str(uid)
            upsert("client_users", flat,
                   id_key="portal_id|client_user_id")
            n += 1
    log.info(f"client_users ({portal_id}): {n} user(s)")


def sync_leaves(configuration, state, portal_id):
    url = build_url(configuration, portal_id, "/leave", version="v3")
    try:
        records = list(paginate_v3(configuration, url, "leaves", per_page=200))
    except ScopeMissing as e:
        log.warning(f"leaves ({portal_id}): scope missing — {e!s}")
        return
    except Exception as e:
        log.warning(f"leaves ({portal_id}): {e!r}")
        return

    for lv in records:
        if not isinstance(lv, dict):
            continue
        lid = lv.get("id") or lv.get("leave_id") or lv.get("id_string")
        if lid is None:
            continue
        flat = flatten_record(lv)
        flat["portal_id"] = portal_id
        flat["leave_id"] = str(lid)
        upsert("leaves", flat, id_key="portal_id|leave_id")
    log.info(f"leaves ({portal_id}): {len(records)} leave record(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  2. Time logs (portal-wide; LMT via fetch_by_modifiedtime)
# ═══════════════════════════════════════════════════════════════════════════
def sync_time_logs(configuration, state, portal_id):
    """Portal-wide time logs via `/logs`. The endpoint accepts:
      - `fetch_by_modifiedtime` (epoch ms) — incremental cursor
      - `date` / `users_list` / `approve` filters
      - 6-month maximum window per call (so on first sync we chunk)

    On first sync we walk back in 30-day windows until we get an empty
    response or hit the portal's earliest log; subsequent runs use the
    `last_modified_long` cursor we checkpointed.
    """
    url = build_url(configuration, portal_id, "/logs")
    state_key = f"{portal_id}__time_logs__last_modified_long"
    cursor = state.get(state_key)
    udf_meta = _udf_meta(configuration, portal_id, "timesheets")

    # `/logs` is one of the strictest Zoho Projects endpoints on parameter
    # patterns (code 6832 on any deviation). The verified-working
    # combination from Zoho's own example URLs:
    #   - users_list=active_users   (literal sentinel; "all" is rejected)
    #   - view_type=flat            (one of: day|week|month|sprint|category|flat)
    #   - date=MM-DD-YYYY           (required; US format even on EU/IN DCs)
    #   - bill_status=all           (one of: all|billable|non_billable)
    #   - fetch_by_modifiedtime=<ms> (incremental cursor — overrides date)
    #
    # For a portal-wide "everything since portal creation" first sync we set
    # `date` to a far-back anchor (01-01-2010). Zoho returns logs spanning
    # the entire history because `view_type=flat` is unbounded forward.
    params = {
        "view_type":    "flat",
        "users_list":   "active_users",
        "bill_status":  "all",
        "date":         "01-01-2010",
    }
    if cursor:
        params["fetch_by_modifiedtime"] = int(cursor)

    try:
        # `/logs` returns nested under `timelogs.date[i].tasklogs/buglogs/general`
        body = api_request(configuration, url, params=params)
    except ScopeMissing as e:
        log.warning(f"time_logs ({portal_id}): scope missing — {e!s}")
        return
    except Exception as e:
        log.warning(f"time_logs ({portal_id}): {e!r}")
        return

    max_modified = int(cursor or 0)
    n_logs = 0
    counter = [0]

    timelogs = body.get("timelogs") if isinstance(body, dict) else None
    if not isinstance(timelogs, dict):
        # Some shapes flatten this further or return `{"date": [...]}` at top.
        timelogs = body if isinstance(body, dict) else {}

    date_buckets = timelogs.get("date") or []
    if not isinstance(date_buckets, list):
        date_buckets = []

    for bucket in date_buckets:
        if not isinstance(bucket, dict):
            continue
        for comp_key, comp_type in (("tasklogs", "task"),
                                     ("buglogs", "bug"),
                                     ("generallogs", "general")):
            logs_for_component = bucket.get(comp_key) or []
            if not isinstance(logs_for_component, list):
                continue
            for entry in logs_for_component:
                if not isinstance(entry, dict):
                    continue
                # Each entry has its own nested `logs` array — that's where
                # the actual log records live.
                inner_logs = entry.get("logs") or [entry]
                if not isinstance(inner_logs, list):
                    inner_logs = []
                # Carry surrounding context onto each log.
                ctx = {k: v for k, v in entry.items()
                       if k not in ("logs",) and not isinstance(v, (list, dict))}
                for raw in inner_logs:
                    if not isinstance(raw, dict):
                        continue
                    lid = raw.get("id") or raw.get("log_id")
                    if lid is None:
                        continue
                    merged = dict(ctx)
                    merged.update(raw)
                    flat = flatten_record(merged)
                    flat["portal_id"] = portal_id
                    flat["log_id"] = str(lid)
                    flat["component_type"] = comp_type
                    flat.setdefault("log_date", bucket.get("date"))
                    upsert("time_logs", flat, id_key="portal_id|log_id")

                    # UDF child rows
                    if udf_meta:
                        _emit_udfs("timesheet_custom_fields", merged,
                                   {"portal_id": portal_id, "log_id": str(lid)},
                                   udf_meta)

                    mod = (raw.get("last_modified_time_long")
                           or raw.get("modified_time_long")
                           or raw.get("last_modified_long"))
                    if mod:
                        try:
                            if int(mod) > max_modified:
                                max_modified = int(mod)
                        except (TypeError, ValueError):
                            pass
                    n_logs += 1
                    _checkpoint_periodically(state, counter)

    if max_modified:
        state[state_key] = max_modified

    log.info(f"time_logs ({portal_id}): {n_logs} log(s); "
             f"cursor → {max_modified}")


# ═══════════════════════════════════════════════════════════════════════════
#  3. Projects (LMT, three status passes)
# ═══════════════════════════════════════════════════════════════════════════
def sync_projects(configuration, state, portal_id, status_filters):
    """List projects across the configured `status_filters`
    (active|archived|template), LMT-filtered when a cursor is available.

    Returns:
      - active_project_ids: ids of projects with status=active (used to drive
                             the per-project fan-out)
      - all_project_ids:    ids across all status passes (used to scope
                             per-project metadata syncs like
                             `timesheet_layouts` and `bug_custom_fields`).
    """
    url = build_url(configuration, portal_id, "/projects/")
    udf_meta = _udf_meta(configuration, portal_id, "projects")
    state_key = f"{portal_id}__projects__last_modified_long"
    cursor = state.get(state_key)

    seen_active: list = []
    seen_all: list = []
    max_modified = int(cursor or 0)
    counter = [0]

    for status in (status_filters or ["active", "archived", "template"]):
        params = {"status": status, "sort_column": "last_modified_time",
                  "sort_order": "ascending"}
        if cursor:
            params["last_modified_time"] = int(cursor)

        try:
            records = paginate_v2(configuration, url, "projects",
                                  params=params, page_size=200)
            n_pass = 0
            for p in records:
                if not isinstance(p, dict):
                    continue
                pid = (p.get("id") or p.get("id_string")
                       or p.get("project_id"))
                if pid is None:
                    continue
                pid = str(pid)
                _emit_record(
                    "projects", p, portal_id,
                    id_col="project_id", record_id=pid,
                    udf_child_table="project_custom_fields",
                    udf_meta=udf_meta,
                    udf_parent_keys={"portal_id": portal_id,
                                     "project_id": pid},
                    entity_type_for_tags="project",
                )
                if status == "active":
                    seen_active.append(pid)
                seen_all.append(pid)

                mod = (p.get("last_modified_time_long")
                       or p.get("modified_time_long"))
                if mod:
                    try:
                        if int(mod) > max_modified:
                            max_modified = int(mod)
                    except (TypeError, ValueError):
                        pass
                n_pass += 1
                _checkpoint_periodically(state, counter)
            log.info(f"projects ({portal_id}, status={status}): "
                     f"{n_pass} project(s)")
        except ScopeMissing as e:
            log.warning(f"projects ({portal_id}, status={status}): "
                        f"scope missing — {e!s}")
            continue
        except Exception as e:
            log.warning(f"projects ({portal_id}, status={status}): {e!r}")
            continue

    if max_modified:
        state[state_key] = max_modified

    # Hard-delete reconciliation on the ACTIVE set only — archived and
    # template projects can disappear from the "active" pass for legitimate
    # reasons (archive transition), and we don't want to delete them.
    seen_set = {(portal_id, pid) for pid in seen_active}
    reconcile_deletes("projects", seen_set, state,
                      key_template={"portal_id": 0, "project_id": 1},
                      state_key_suffix=f"__{portal_id}__active")

    return seen_active, list(dict.fromkeys(seen_all))


# ── Per-project participant junctions ───────────────────────────────────────
def sync_project_users(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/users/")
    try:
        body = api_request(configuration, url)
    except (ScopeMissing, Exception) as e:
        log.fine(f"project_users ({portal_id}/{project_id}): {e!r}")
        return

    raw = body.get("users") if isinstance(body, dict) else (body or [])
    if not isinstance(raw, list):
        raw = []
    for u in raw:
        if not isinstance(u, dict):
            continue
        uid = u.get("id") or u.get("user_id") or u.get("zpuid")
        if uid is None:
            continue
        flat = flatten_record(u)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["user_id"] = str(uid)
        upsert("project_users", flat,
               id_key="portal_id|project_id|user_id")


def sync_project_clients(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/clients/")
    try:
        body = api_request(configuration, url)
    except (ScopeMissing, Exception) as e:
        log.fine(f"project_clients ({portal_id}/{project_id}): {e!r}")
        return

    raw = body.get("clients") if isinstance(body, dict) else (body or [])
    if not isinstance(raw, list):
        raw = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        cid = c.get("id") or c.get("client_id")
        if cid is None:
            continue
        flat = flatten_record(c)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["client_id"] = str(cid)
        upsert("project_clients", flat,
               id_key="portal_id|project_id|client_id")


# ═══════════════════════════════════════════════════════════════════════════
#  4. Milestones (LMT)
# ═══════════════════════════════════════════════════════════════════════════
def sync_milestones(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/milestones/")
    state_key = f"{portal_id}__{project_id}__milestones__last_modified_long"
    cursor = state.get(state_key)

    # Zoho returns milestones grouped by status by default; pass
    # `status=all` to get the union.
    params = {"status": "all"}
    if cursor:
        params["last_modified_time"] = int(cursor)

    try:
        records = paginate_v2(configuration, url, "milestones",
                              params=params, page_size=200)
        max_modified = int(cursor or 0)
        n = 0
        for m in records:
            if not isinstance(m, dict):
                continue
            mid = m.get("id") or m.get("milestone_id") or m.get("id_string")
            if mid is None:
                continue
            mid = str(mid)
            _emit_record(
                "milestones", m, portal_id,
                id_col="milestone_id", record_id=mid,
                extra_cols={"project_id": str(project_id)},
                id_key="portal_id|project_id|milestone_id",
                entity_type_for_tags="milestone",
            )
            mod = m.get("last_modified_time_long") or m.get("modified_time_long")
            if mod:
                try:
                    if int(mod) > max_modified:
                        max_modified = int(mod)
                except (TypeError, ValueError):
                    pass
            n += 1
        if max_modified:
            state[state_key] = max_modified
        log.fine(f"milestones ({portal_id}/{project_id}): {n} record(s)")
    except (ScopeMissing, Exception) as e:
        log.fine(f"milestones ({portal_id}/{project_id}): {e!r}")


# ═══════════════════════════════════════════════════════════════════════════
#  5. Tasklists (LMT)
# ═══════════════════════════════════════════════════════════════════════════
def sync_tasklists(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/tasklists/")
    state_key = f"{portal_id}__{project_id}__tasklists__last_modified_long"
    cursor = state.get(state_key)

    # `flag=internal` returns all tasklists regardless of completion;
    # default returns only open ones.
    params = {"flag": "internal"}
    if cursor:
        params["last_modified_time"] = int(cursor)

    try:
        records = paginate_v2(configuration, url, "tasklists",
                              params=params, page_size=200)
        max_modified = int(cursor or 0)
        n = 0
        for tl in records:
            if not isinstance(tl, dict):
                continue
            tlid = tl.get("id") or tl.get("tasklist_id") or tl.get("id_string")
            if tlid is None:
                continue
            tlid = str(tlid)
            _emit_record(
                "tasklists", tl, portal_id,
                id_col="tasklist_id", record_id=tlid,
                extra_cols={"project_id": str(project_id)},
                id_key="portal_id|project_id|tasklist_id",
                entity_type_for_tags="tasklist",
            )
            mod = tl.get("last_modified_time_long") or tl.get("modified_time_long")
            if mod:
                try:
                    if int(mod) > max_modified:
                        max_modified = int(mod)
                except (TypeError, ValueError):
                    pass
            n += 1
        if max_modified:
            state[state_key] = max_modified
        log.fine(f"tasklists ({portal_id}/{project_id}): {n} record(s)")
    except (ScopeMissing, Exception) as e:
        log.fine(f"tasklists ({portal_id}/{project_id}): {e!r}")


# ═══════════════════════════════════════════════════════════════════════════
#  6. Tasks (LMT) + fan-out
# ═══════════════════════════════════════════════════════════════════════════
def sync_tasks(configuration, state, portal_id, project_id):
    """List tasks (LMT) and for each, fan out to subtasks, comments,
    attachments, followers, dependencies (from Task Details), activities.

    Returns the list of task IDs synced (so the caller can drive the
    task status history sync at project scope)."""
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/tasks/")
    state_key = f"{portal_id}__{project_id}__tasks__last_modified_long"
    cursor = state.get(state_key)
    udf_meta = _udf_meta(configuration, portal_id, "tasks")

    params = {}
    if cursor:
        params["last_modified_time"] = int(cursor)

    sync_attachments_meta = config_bool(configuration, "sync_attachments_meta", True)

    task_ids: list = []
    max_modified = int(cursor or 0)
    counter = [0]
    n = 0

    try:
        records = paginate_v2(configuration, url, "tasks",
                              params=params, page_size=200)
        for t in records:
            if not isinstance(t, dict):
                continue
            tid = t.get("id") or t.get("task_id") or t.get("id_string")
            if tid is None:
                continue
            tid = str(tid)
            task_ids.append(tid)

            _emit_record(
                "tasks", t, portal_id,
                id_col="task_id", record_id=tid,
                extra_cols={"project_id": str(project_id)},
                id_key="portal_id|project_id|task_id",
                udf_child_table="task_custom_fields",
                udf_meta=udf_meta,
                udf_parent_keys={"portal_id": portal_id,
                                 "project_id": str(project_id),
                                 "task_id": tid},
                entity_type_for_tags="task",
                # These are emitted to child tables below; don't JSON-blob
                # them onto the parent row.
                drop_keys={"dependency", "dependencies", "followers",
                           "followers_list", "details", "assignees",
                           "owners"},
            )

            mod = t.get("last_modified_time_long") or t.get("modified_time_long")
            if mod:
                try:
                    if int(mod) > max_modified:
                        max_modified = int(mod)
                except (TypeError, ValueError):
                    pass

            # Extract dependencies + followers from the LIST response
            # itself — Zoho already returns them on each task object. Doing
            # so eliminates two per-task detail GETs, which would otherwise
            # blow through the 100-req/2-min rolling-throttle cap on the
            # task-detail endpoint.
            _emit_task_dependencies_from_record(portal_id, project_id, tid, t)
            _emit_task_followers_from_record(portal_id, project_id, tid, t)
            _emit_task_owners_from_record(portal_id, project_id, tid, t)

            _sync_subtasks_for_task(configuration, portal_id, project_id, tid)
            _sync_task_comments_for_task(configuration, state, portal_id,
                                         project_id, tid)
            if sync_attachments_meta:
                _sync_task_attachments_for_task(configuration, portal_id,
                                                project_id, tid)
            _sync_task_activities_for_task(configuration, portal_id,
                                           project_id, tid)

            n += 1
            _checkpoint_periodically(state, counter)

        if max_modified:
            state[state_key] = max_modified
        log.info(f"tasks ({portal_id}/{project_id}): {n} task(s)")
    except (ScopeMissing, Exception) as e:
        log.warning(f"tasks ({portal_id}/{project_id}): {e!r}")

    return task_ids


def _sync_subtasks_for_task(configuration, portal_id, project_id, task_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/tasks/{task_id}/subtasks/")
    try:
        records = list(paginate_v2(configuration, url, "tasks",
                                    page_size=200))
    except (ScopeMissing, Exception):
        return
    for st in records:
        if not isinstance(st, dict):
            continue
        sid = st.get("id") or st.get("subtask_id") or st.get("id_string")
        if sid is None:
            continue
        flat = flatten_record(st)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["task_id"] = str(task_id)
        flat["subtask_id"] = str(sid)
        upsert("subtasks", flat,
               id_key="portal_id|project_id|task_id|subtask_id")


def _sync_task_comments_for_task(configuration, state, portal_id,
                                 project_id, task_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/tasks/{task_id}/comments/")
    state_key = (f"{portal_id}__{project_id}__{task_id}__"
                 f"task_comments__last_modified_long")
    cursor = state.get(state_key)

    params = {}
    if cursor:
        params["last_modified_time"] = int(cursor)

    try:
        records = paginate_v2(configuration, url, "comments",
                              params=params, page_size=200)
        max_modified = int(cursor or 0)
        for c in records:
            if not isinstance(c, dict):
                continue
            cid = c.get("id") or c.get("comment_id") or c.get("id_string")
            if cid is None:
                continue
            flat = flatten_record(c)
            flat["portal_id"] = portal_id
            flat["project_id"] = str(project_id)
            flat["task_id"] = str(task_id)
            flat["comment_id"] = str(cid)
            upsert("task_comments", flat,
                   id_key="portal_id|project_id|task_id|comment_id")
            mod = c.get("last_modified_time_long") or c.get("modified_time_long")
            if mod:
                try:
                    if int(mod) > max_modified:
                        max_modified = int(mod)
                except (TypeError, ValueError):
                    pass
        if max_modified:
            state[state_key] = max_modified
    except (ScopeMissing, Exception):
        pass


def _sync_task_attachments_for_task(configuration, portal_id, project_id,
                                    task_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/tasks/{task_id}/attachments/")
    try:
        body = api_request(configuration, url)
    except (ScopeMissing, Exception):
        return
    raw = []
    if isinstance(body, dict):
        # V2 sometimes returns the list at top level, sometimes under
        # `attachment_details`; V3 returns `attachment` (singular).
        for k in ("attachment_details", "attachment", "attachments",
                  "data", "result"):
            v = body.get(k)
            if isinstance(v, list):
                raw = v
                break
    elif isinstance(body, list):
        raw = body
    for a in raw:
        if not isinstance(a, dict):
            continue
        aid = (a.get("id") or a.get("attachment_id") or a.get("file_id")
               or a.get("id_string"))
        if aid is None:
            continue
        flat = flatten_record(a)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["task_id"] = str(task_id)
        flat["attachment_id"] = str(aid)
        upsert("task_attachments", flat,
               id_key="portal_id|project_id|task_id|attachment_id")


def _emit_task_owners_from_record(portal_id, project_id, task_id, record):
    """Emit task_owners rows from the `details.owners` (or top-level
    `assignees`) array on the LIST-endpoint task object — no extra API
    call needed.

    Zoho exposes assignees in two parallel shapes:
      - `details.owners` (V2): `[{name, zpuid, full_name, work, email}]`
      - `assignees` (V3): `[{id, name, email, ...}]`
    """
    if not isinstance(record, dict):
        return
    details = record.get("details")
    owners = None
    if isinstance(details, dict):
        owners = details.get("owners")
    if not isinstance(owners, list):
        owners = record.get("assignees") or record.get("owners")
    if not isinstance(owners, list):
        return
    for o in owners:
        if not isinstance(o, dict):
            continue
        uid = (o.get("zpuid") or o.get("id") or o.get("user_id")
               or o.get("id_string"))
        if uid is None:
            continue
        flat = flatten_record(o)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["task_id"] = str(task_id)
        flat["user_id"] = str(uid)
        upsert("task_owners", flat,
               id_key="portal_id|project_id|task_id|user_id")


def _emit_forum_followers_from_record(portal_id, project_id, forum_id, record):
    """Emit forum_followers rows from the LIST-endpoint forum object."""
    if not isinstance(record, dict):
        return
    followers = record.get("followers") or record.get("follower_list") or []
    if not isinstance(followers, list):
        return
    for f in followers:
        if not isinstance(f, dict):
            continue
        uid = (f.get("zpuid") or f.get("id") or f.get("user_id")
               or f.get("id_string"))
        if uid is None:
            continue
        flat = flatten_record(f)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["forum_id"] = str(forum_id)
        flat["user_id"] = str(uid)
        upsert("forum_followers", flat,
               id_key="portal_id|project_id|forum_id|user_id")


def _emit_forum_attachments_from_record(portal_id, project_id, forum_id,
                                        record):
    """Emit forum_attachments rows from the LIST-endpoint forum object."""
    if not isinstance(record, dict):
        return
    atts = record.get("attachments") or record.get("attachment_details") or []
    if not isinstance(atts, list):
        return
    for a in atts:
        if not isinstance(a, dict):
            continue
        aid = (a.get("id") or a.get("attachment_id") or a.get("file_id")
               or a.get("id_string"))
        if aid is None:
            continue
        flat = flatten_record(a)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["forum_id"] = str(forum_id)
        flat["attachment_id"] = str(aid)
        upsert("forum_attachments", flat,
               id_key="portal_id|project_id|forum_id|attachment_id")


def _emit_task_followers_from_record(portal_id, project_id, task_id, record):
    """Emit task_followers rows from the followers array already present on
    the LIST-endpoint task object — no extra API call needed."""
    if not isinstance(record, dict):
        return
    followers = record.get("followers") or record.get("followers_list") or []
    if isinstance(followers, dict):
        # Some shapes return {"USERS": [...]} or {"users": [...]}
        for k in ("USERS", "users", "user_list", "list"):
            v = followers.get(k)
            if isinstance(v, list):
                followers = v
                break
        else:
            followers = []
    if not isinstance(followers, list):
        return
    for f in followers:
        if not isinstance(f, dict):
            continue
        uid = (f.get("zpuid") or f.get("id") or f.get("user_id")
               or f.get("id_string"))
        if uid is None:
            continue
        flat = flatten_record(f)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["task_id"] = str(task_id)
        flat["user_id"] = str(uid)
        upsert("task_followers", flat,
               id_key="portal_id|project_id|task_id|user_id")


def _sync_task_activities_for_task(configuration, portal_id, project_id, task_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/tasks/{task_id}/activities/")
    try:
        body = api_request(configuration, url)
    except (ScopeMissing, Exception):
        return
    raw = []
    if isinstance(body, dict):
        for k in ("activities", "data", "result"):
            v = body.get(k)
            if isinstance(v, list):
                raw = v
                break
    for a in raw:
        if not isinstance(a, dict):
            continue
        aid = (a.get("id") or a.get("activity_id") or a.get("id_string"))
        if aid is None:
            continue
        flat = flatten_record(a)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["task_id"] = str(task_id)
        flat["activity_id"] = str(aid)
        upsert("task_activities", flat,
               id_key="portal_id|project_id|task_id|activity_id")


def _emit_task_dependencies_from_record(portal_id, project_id, task_id,
                                        record):
    """Emit task_dependencies rows from the `dependency` block already
    present on the LIST-endpoint task object — no extra API call needed.

    The dependency shape is typically:
       "dependency": {
         "successor":   [{"id": "...", "type": "FS", "lag": 0}, ...],
         "predecessor": [{...}, ...]
       }
    Or alternatively a flat list. We accept both shapes."""
    if not isinstance(record, dict):
        return
    dep = record.get("dependency") or record.get("dependencies")
    if not dep:
        return

    def _emit_one(dep_record, direction):
        if not isinstance(dep_record, dict):
            return
        target = (dep_record.get("id") or dep_record.get("task_id")
                  or dep_record.get("id_string")
                  or dep_record.get("dependent_task_id"))
        if target is None:
            return
        flat = flatten_record(dep_record)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["task_id"] = str(task_id)
        flat["depends_on_task_id"] = str(target)
        flat["direction"] = direction
        upsert("task_dependencies", flat,
               id_key="portal_id|project_id|task_id|depends_on_task_id")

    if isinstance(dep, dict):
        for k, direction in (("successor", "successor"),
                              ("predecessor", "predecessor")):
            for d in (dep.get(k) or []):
                _emit_one(d, direction)
    elif isinstance(dep, list):
        for d in dep:
            _emit_one(d, "unknown")


def sync_task_status_history(configuration, state, portal_id, project_id):
    """V3 task status history at project scope, LMT-filtered."""
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/taskstatushistory",
                    version="v3")
    state_key = (f"{portal_id}__{project_id}__"
                 f"task_status_history__last_modified_long")
    cursor = state.get(state_key)
    params = {}
    if cursor:
        params["last_modified_time"] = int(cursor)

    try:
        records = paginate_v3(configuration, url, "task_status_history",
                              params=params, per_page=200)
        max_modified = int(cursor or 0)
        n = 0
        for h in records:
            if not isinstance(h, dict):
                continue
            hid = (h.get("id") or h.get("history_id") or h.get("id_string"))
            if hid is None:
                continue
            tid = h.get("task_id") or h.get("task") or {}
            if isinstance(tid, dict):
                tid = tid.get("id") or tid.get("id_string")
            flat = flatten_record(h)
            flat["portal_id"] = portal_id
            flat["project_id"] = str(project_id)
            flat["task_id"] = str(tid) if tid is not None else None
            flat["history_id"] = str(hid)
            upsert("task_status_history", flat,
                   id_key="portal_id|project_id|task_id|history_id")
            mod = h.get("last_modified_time_long") or h.get("modified_time_long")
            if mod:
                try:
                    if int(mod) > max_modified:
                        max_modified = int(mod)
                except (TypeError, ValueError):
                    pass
            n += 1
        if max_modified:
            state[state_key] = max_modified
        log.fine(f"task_status_history ({portal_id}/{project_id}): {n}")
    except (ScopeMissing, Exception) as e:
        log.fine(f"task_status_history ({portal_id}/{project_id}): {e!r}")


# ═══════════════════════════════════════════════════════════════════════════
#  7. Bugs (full refresh per project) + fan-out
# ═══════════════════════════════════════════════════════════════════════════
def sync_bugs(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/bugs/")
    udf_meta = _udf_meta(configuration, portal_id, "bugs",
                         project_id=str(project_id))

    sync_attachments_meta = config_bool(configuration, "sync_attachments_meta", True)
    sync_bug_task = config_bool(configuration, "sync_bug_task_associations", True)

    bug_ids: list = []
    n = 0
    counter = [0]
    try:
        # No `last_modified_time` filter on this endpoint — full refresh.
        records = paginate_v2(configuration, url, "bugs", page_size=200)
        for b in records:
            if not isinstance(b, dict):
                continue
            bid = b.get("id") or b.get("bug_id") or b.get("id_string")
            if bid is None:
                continue
            bid = str(bid)
            bug_ids.append(bid)

            _emit_record(
                "bugs", b, portal_id,
                id_col="bug_id", record_id=bid,
                extra_cols={"project_id": str(project_id)},
                id_key="portal_id|project_id|bug_id",
                udf_child_table="bug_custom_fields",
                udf_meta=udf_meta,
                udf_parent_keys={"portal_id": portal_id,
                                 "project_id": str(project_id),
                                 "bug_id": bid},
                entity_type_for_tags="bug",
            )

            _sync_bug_comments_for_bug(configuration, portal_id, project_id, bid)
            if sync_attachments_meta:
                _sync_bug_attachments_for_bug(configuration, portal_id,
                                              project_id, bid)
            _sync_bug_resolution_for_bug(configuration, portal_id, project_id, bid)
            _sync_bug_timer_for_bug(configuration, portal_id, project_id, bid)
            _sync_bug_followers_for_bug(configuration, portal_id, project_id, bid)
            _sync_bug_activities_for_bug(configuration, portal_id, project_id, bid)
            if sync_bug_task:
                _sync_bug_task_associations_for_bug(configuration, portal_id,
                                                   project_id, bid)
            n += 1
            _checkpoint_periodically(state, counter)
    except (ScopeMissing, Exception) as e:
        log.warning(f"bugs ({portal_id}/{project_id}): {e!r}")

    # Hard-delete reconciliation scoped to this (portal, project).
    seen_set = {(portal_id, str(project_id), bid) for bid in bug_ids}
    reconcile_deletes("bugs", seen_set, state,
                      key_template={"portal_id": 0, "project_id": 1, "bug_id": 2},
                      state_key_suffix=f"__{portal_id}__{project_id}")

    log.info(f"bugs ({portal_id}/{project_id}): {n} bug(s)")


def _sync_bug_comments_for_bug(configuration, portal_id, project_id, bug_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/bugs/{bug_id}/comments/")
    try:
        records = list(paginate_v2(configuration, url, "comments",
                                    page_size=200))
    except (ScopeMissing, Exception):
        return
    for c in records:
        if not isinstance(c, dict):
            continue
        cid = c.get("id") or c.get("comment_id") or c.get("id_string")
        if cid is None:
            continue
        flat = flatten_record(c)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["bug_id"] = str(bug_id)
        flat["comment_id"] = str(cid)
        upsert("bug_comments", flat,
               id_key="portal_id|project_id|bug_id|comment_id")


def _sync_bug_attachments_for_bug(configuration, portal_id, project_id, bug_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/bugs/{bug_id}/attachments/")
    try:
        body = api_request(configuration, url)
    except (ScopeMissing, Exception):
        return
    raw = []
    if isinstance(body, dict):
        # V2 wraps the attachments under `attachment_details`; V3 returns
        # `attachment`. The generic `attachments` / `data` are fallbacks.
        for k in ("attachment_details", "attachment", "attachments",
                  "data", "result"):
            v = body.get(k)
            if isinstance(v, list):
                raw = v
                break
    for a in raw:
        if not isinstance(a, dict):
            continue
        aid = (a.get("id") or a.get("attachment_id") or a.get("file_id")
               or a.get("id_string"))
        if aid is None:
            continue
        flat = flatten_record(a)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["bug_id"] = str(bug_id)
        flat["attachment_id"] = str(aid)
        upsert("bug_attachments", flat,
               id_key="portal_id|project_id|bug_id|attachment_id")


def _sync_bug_resolution_for_bug(configuration, portal_id, project_id, bug_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/bugs/{bug_id}/resolution/")
    try:
        body = api_request(configuration, url)
    except (ScopeMissing, Exception):
        return
    resolution = None
    if isinstance(body, dict):
        # V2 wraps the resolution payload under `resolution_details`;
        # `resolution` is the V3/older fallback shape.
        inner = body.get("resolution_details")
        if isinstance(inner, list) and inner and isinstance(inner[0], dict):
            resolution = inner[0]
        elif isinstance(inner, dict):
            resolution = inner
        else:
            resolution = body.get("resolution") or body
    if not isinstance(resolution, dict) or not resolution:
        return
    # Strip noise top-level keys when we used the body itself.
    flat = flatten_record(resolution)
    flat["portal_id"] = portal_id
    flat["project_id"] = str(project_id)
    flat["bug_id"] = str(bug_id)
    upsert("bug_resolutions", flat,
           id_key="portal_id|project_id|bug_id")


def _sync_bug_timer_for_bug(configuration, portal_id, project_id, bug_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/bugs/{bug_id}/timer")
    try:
        body = api_request(configuration, url)
    except (ScopeMissing, Exception):
        return
    timer = None
    if isinstance(body, dict):
        timer = body.get("timer") or body
    if not isinstance(timer, dict) or not timer:
        return
    flat = flatten_record(timer)
    flat["portal_id"] = portal_id
    flat["project_id"] = str(project_id)
    flat["bug_id"] = str(bug_id)
    upsert("bug_timers", flat,
           id_key="portal_id|project_id|bug_id")


def _sync_bug_followers_for_bug(configuration, portal_id, project_id, bug_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/bugs/{bug_id}/bugfollowers/")
    try:
        body = api_request(configuration, url)
    except (ScopeMissing, Exception):
        return
    followers = []
    if isinstance(body, dict):
        # V2 wraps the followers under `issue_follower` (singular, with i —
        # internal Zoho slug). V3 uses `followers`. Both fallbacks kept.
        for k in ("issue_follower", "followers", "bugfollowers", "data"):
            v = body.get(k)
            if isinstance(v, list):
                followers = v
                break
    for f in followers:
        if not isinstance(f, dict):
            continue
        uid = (f.get("zpuid") or f.get("id") or f.get("user_id")
               or f.get("id_string"))
        if uid is None:
            continue
        flat = flatten_record(f)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["bug_id"] = str(bug_id)
        flat["user_id"] = str(uid)
        upsert("bug_followers", flat,
               id_key="portal_id|project_id|bug_id|user_id")


def _sync_bug_activities_for_bug(configuration, portal_id, project_id, bug_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/bugs/{bug_id}/activities/")
    try:
        body = api_request(configuration, url)
    except (ScopeMissing, Exception):
        return
    raw = []
    if isinstance(body, dict):
        # V2 returns activities under `activity_details`; `activities` is
        # the cross-version fallback.
        for k in ("activity_details", "activities", "data"):
            v = body.get(k)
            if isinstance(v, list):
                raw = v
                break
    for a in raw:
        if not isinstance(a, dict):
            continue
        aid = (a.get("id") or a.get("activity_id") or a.get("id_string"))
        if aid is None:
            continue
        flat = flatten_record(a)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["bug_id"] = str(bug_id)
        flat["activity_id"] = str(aid)
        upsert("bug_activities", flat,
               id_key="portal_id|project_id|bug_id|activity_id")


def _sync_bug_task_associations_for_bug(configuration, portal_id,
                                        project_id, bug_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/bugs/{bug_id}/tasks",
                    version="v3")
    try:
        # V3 wraps the linked-task list under `associated_tasks`;
        # `tasks` is the fallback.
        records = list(paginate_v3(configuration, url, "associated_tasks",
                                    per_page=200))
        if not records:
            records = list(paginate_v3(configuration, url, "tasks",
                                        per_page=200))
    except (ScopeMissing, Exception):
        return
    for t in records:
        if not isinstance(t, dict):
            continue
        tid = t.get("id") or t.get("task_id") or t.get("id_string")
        if tid is None:
            continue
        flat = flatten_record(t)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["bug_id"] = str(bug_id)
        flat["task_id"] = str(tid)
        upsert("bug_task_associations", flat,
               id_key="portal_id|project_id|bug_id|task_id")


# ═══════════════════════════════════════════════════════════════════════════
#  8. Events / forums / documents (full refresh)
# ═══════════════════════════════════════════════════════════════════════════
def sync_events(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/events/")
    try:
        records = list(paginate_v2(configuration, url, "events",
                                    page_size=200))
    except (ScopeMissing, Exception) as e:
        log.fine(f"events ({portal_id}/{project_id}): {e!r}")
        return
    for ev in records:
        if not isinstance(ev, dict):
            continue
        eid = ev.get("id") or ev.get("event_id") or ev.get("id_string")
        if eid is None:
            continue
        eid = str(eid)
        flat = flatten_record(ev)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["event_id"] = eid
        upsert("events", flat, id_key="portal_id|project_id|event_id")

        # Per-event comments (V3 endpoint). Skip silently if not exposed.
        c_url = build_url(configuration, portal_id,
                          f"/projects/{project_id}/events/{eid}/comments",
                          version="v3")
        try:
            comments = list(paginate_v3(configuration, c_url, "comments",
                                         per_page=200))
        except (ScopeMissing, Exception):
            continue
        for c in comments:
            if not isinstance(c, dict):
                continue
            cid = c.get("id") or c.get("comment_id") or c.get("id_string")
            if cid is None:
                continue
            cflat = flatten_record(c)
            cflat["portal_id"] = portal_id
            cflat["project_id"] = str(project_id)
            cflat["event_id"] = eid
            cflat["comment_id"] = str(cid)
            upsert("event_comments", cflat,
                   id_key="portal_id|project_id|event_id|comment_id")


def sync_forums(configuration, state, portal_id, project_id):
    # 1) forum categories
    cat_url = build_url(configuration, portal_id,
                        f"/projects/{project_id}/categories/")
    try:
        body = api_request(configuration, cat_url)
        categories = []
        if isinstance(body, dict):
            for k in ("categories", "data"):
                v = body.get(k)
                if isinstance(v, list):
                    categories = v
                    break
        for c in categories:
            if not isinstance(c, dict):
                continue
            cid = c.get("id") or c.get("category_id") or c.get("id_string")
            if cid is None:
                continue
            flat = flatten_record(c)
            flat["portal_id"] = portal_id
            flat["project_id"] = str(project_id)
            flat["category_id"] = str(cid)
            upsert("forum_categories", flat,
                   id_key="portal_id|project_id|category_id")
    except (ScopeMissing, Exception):
        pass

    # 2) forums
    f_url = build_url(configuration, portal_id,
                      f"/projects/{project_id}/forums/")
    try:
        records = list(paginate_v2(configuration, f_url, "forums",
                                    page_size=200))
    except (ScopeMissing, Exception) as e:
        log.fine(f"forums ({portal_id}/{project_id}): {e!r}")
        return
    for fo in records:
        if not isinstance(fo, dict):
            continue
        fid = fo.get("id") or fo.get("forum_id") or fo.get("id_string")
        if fid is None:
            continue
        fid = str(fid)
        _emit_record(
            "forums", fo, portal_id,
            id_col="forum_id", record_id=fid,
            extra_cols={"project_id": str(project_id)},
            id_key="portal_id|project_id|forum_id",
            entity_type_for_tags="forum",
            drop_keys={"followers", "attachments"},
        )
        # Forums list response embeds followers + attachments arrays per
        # forum object. Extract them rather than JSON-blobbing onto parent.
        _emit_forum_followers_from_record(portal_id, project_id, fid, fo)
        _emit_forum_attachments_from_record(portal_id, project_id, fid, fo)

        # Per-forum comments
        c_url = build_url(configuration, portal_id,
                          f"/projects/{project_id}/forums/{fid}/comments/")
        try:
            comments = list(paginate_v2(configuration, c_url, "comments",
                                         page_size=200))
        except (ScopeMissing, Exception):
            continue
        for c in comments:
            if not isinstance(c, dict):
                continue
            cid = c.get("id") or c.get("comment_id") or c.get("id_string")
            if cid is None:
                continue
            flat = flatten_record(c)
            flat["portal_id"] = portal_id
            flat["project_id"] = str(project_id)
            flat["forum_id"] = fid
            flat["comment_id"] = str(cid)
            upsert("forum_comments", flat,
                   id_key="portal_id|project_id|forum_id|comment_id")


def sync_documents(configuration, state, portal_id, project_id):
    # 1) folders
    fo_url = build_url(configuration, portal_id,
                       f"/projects/{project_id}/folders/")
    try:
        body = api_request(configuration, fo_url)
        folders = []
        if isinstance(body, dict):
            for k in ("folders", "data"):
                v = body.get(k)
                if isinstance(v, list):
                    folders = v
                    break
        for f in folders:
            if not isinstance(f, dict):
                continue
            fid = f.get("id") or f.get("folder_id") or f.get("id_string")
            if fid is None:
                continue
            flat = flatten_record(f)
            flat["portal_id"] = portal_id
            flat["project_id"] = str(project_id)
            flat["folder_id"] = str(fid)
            upsert("folders", flat,
                   id_key="portal_id|project_id|folder_id")
    except (ScopeMissing, Exception):
        pass

    # 2) documents — V2 returns the list under `dataobj` (not `documents`).
    d_url = build_url(configuration, portal_id,
                      f"/projects/{project_id}/documents/")
    try:
        records = list(paginate_v2(configuration, d_url, "dataobj",
                                    page_size=200))
        if not records:
            # Fallback in case Zoho ever normalises the key.
            records = list(paginate_v2(configuration, d_url, "documents",
                                        page_size=200))
    except (ScopeMissing, Exception) as e:
        log.fine(f"documents ({portal_id}/{project_id}): {e!r}")
        return
    for d in records:
        if not isinstance(d, dict):
            continue
        did = d.get("id") or d.get("document_id") or d.get("id_string")
        if did is None:
            continue
        did = str(did)
        flat = flatten_record(d)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["document_id"] = did
        upsert("documents", flat,
               id_key="portal_id|project_id|document_id")

        # Document versions live under the same path with the document id.
        v_url = build_url(configuration, portal_id,
                          f"/projects/{project_id}/documents/{did}/")
        try:
            v_body = api_request(configuration, v_url)
        except (ScopeMissing, Exception):
            continue
        versions = []
        if isinstance(v_body, dict):
            for k in ("versions", "document_versions", "data"):
                v = v_body.get(k)
                if isinstance(v, list):
                    versions = v
                    break
            if not versions:
                # Some endpoints wrap each version under `documents[0].versions`.
                docs = v_body.get("documents") or []
                if isinstance(docs, list) and docs and isinstance(docs[0], dict):
                    versions = docs[0].get("versions") or []
        for v in versions:
            if not isinstance(v, dict):
                continue
            vid = (v.get("version_id") or v.get("id") or v.get("id_string"))
            if vid is None:
                continue
            vflat = flatten_record(v)
            vflat["portal_id"] = portal_id
            vflat["project_id"] = str(project_id)
            vflat["document_id"] = did
            vflat["version_id"] = str(vid)
            upsert("document_versions", vflat,
                   id_key="portal_id|project_id|document_id|version_id")


# ═══════════════════════════════════════════════════════════════════════════
#  9. Activity / status feeds (append-only; max-id-seen cursor)
# ═══════════════════════════════════════════════════════════════════════════
def _max_id(records, *id_keys):
    best = None
    for r in records:
        if not isinstance(r, dict):
            continue
        for k in id_keys:
            v = r.get(k)
            if v is None:
                continue
            try:
                vi = int(v)
            except (TypeError, ValueError):
                continue
            if best is None or vi > best:
                best = vi
    return best


def sync_project_activities(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/activities/")
    state_key = (f"{portal_id}__{project_id}__"
                 f"project_activities__max_id")
    seen_max = state.get(state_key)
    try:
        records = list(paginate_v2(configuration, url, "activities",
                                    page_size=200))
    except (ScopeMissing, Exception) as e:
        log.fine(f"project_activities ({portal_id}/{project_id}): {e!r}")
        return
    new_max = None
    for a in records:
        if not isinstance(a, dict):
            continue
        aid = a.get("id") or a.get("activity_id") or a.get("id_string")
        if aid is None:
            continue
        # Skip already-seen entries on incremental runs.
        try:
            if seen_max is not None and int(aid) <= int(seen_max):
                continue
        except (TypeError, ValueError):
            pass
        flat = flatten_record(a)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["activity_id"] = str(aid)
        upsert("project_activities", flat,
               id_key="portal_id|project_id|activity_id")
        try:
            iv = int(aid)
            if new_max is None or iv > new_max:
                new_max = iv
        except (TypeError, ValueError):
            pass
    if new_max is not None:
        state[state_key] = new_max


# ═══════════════════════════════════════════════════════════════════════════
#  Project comments + event comments
# ═══════════════════════════════════════════════════════════════════════════
def sync_project_comments(configuration, state, portal_id, project_id):
    """Project-wall comments. Distinct from task/bug/forum comments —
    these are top-level project discussion threads."""
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/projectcomments/")
    try:
        records = list(paginate_v2(configuration, url, "comments",
                                    page_size=200))
    except (ScopeMissing, Exception) as e:
        log.fine(f"project_comments ({portal_id}/{project_id}): {e!r}")
        return
    for c in records:
        if not isinstance(c, dict):
            continue
        cid = c.get("id") or c.get("comment_id") or c.get("id_string")
        if cid is None:
            continue
        flat = flatten_record(c)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["comment_id"] = str(cid)
        upsert("project_comments", flat,
               id_key="portal_id|project_id|comment_id")


def sync_project_statuses(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/statuses/")
    state_key = (f"{portal_id}__{project_id}__"
                 f"project_statuses__max_id")
    seen_max = state.get(state_key)
    try:
        records = list(paginate_v2(configuration, url, "statuses",
                                    page_size=200))
    except (ScopeMissing, Exception) as e:
        log.fine(f"project_statuses ({portal_id}/{project_id}): {e!r}")
        return
    new_max = None
    for s in records:
        if not isinstance(s, dict):
            continue
        sid = s.get("id") or s.get("status_id") or s.get("id_string")
        if sid is None:
            continue
        try:
            if seen_max is not None and int(sid) <= int(seen_max):
                continue
        except (TypeError, ValueError):
            pass
        flat = flatten_record(s)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["status_id"] = str(sid)
        # Tag association — status entries can carry tags too.
        _emit_record(
            "project_statuses", s, portal_id,
            id_col="status_id", record_id=sid,
            extra_cols={"project_id": str(project_id)},
            id_key="portal_id|project_id|status_id",
            entity_type_for_tags="status",
        )
        try:
            iv = int(sid)
            if new_max is None or iv > new_max:
                new_max = iv
        except (TypeError, ValueError):
            pass
    if new_max is not None:
        state[state_key] = new_max


# ═══════════════════════════════════════════════════════════════════════════
#  Custom views (task + bug) — small dim tables
# ═══════════════════════════════════════════════════════════════════════════
def sync_task_custom_views(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/tasks/views")
    try:
        records = list(paginate_v2(configuration, url, "views",
                                    page_size=200))
    except (ScopeMissing, Exception) as e:
        log.fine(f"task_custom_views ({portal_id}/{project_id}): {e!r}")
        return
    for v in records:
        if not isinstance(v, dict):
            continue
        vid = v.get("id") or v.get("view_id") or v.get("id_string")
        if vid is None:
            continue
        flat = flatten_record(v)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["view_id"] = str(vid)
        upsert("task_custom_views", flat,
               id_key="portal_id|project_id|view_id")


def sync_bug_custom_views(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/bugs/customviews/")
    try:
        body = api_request(configuration, url)
    except (ScopeMissing, Exception) as e:
        log.fine(f"bug_custom_views ({portal_id}/{project_id}): {e!r}")
        return
    raw = []
    if isinstance(body, dict):
        # V2 returns `cview_details`; older shapes use `customviews` or
        # `views`. Try all.
        for k in ("cview_details", "customviews", "custom_views", "views",
                  "data"):
            val = body.get(k)
            if isinstance(val, list):
                raw = val
                break
    elif isinstance(body, list):
        raw = body
    for v in raw:
        if not isinstance(v, dict):
            continue
        vid = v.get("id") or v.get("view_id") or v.get("cview_id") \
              or v.get("id_string")
        if vid is None:
            continue
        flat = flatten_record(v)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["view_id"] = str(vid)
        upsert("bug_custom_views", flat,
               id_key="portal_id|project_id|view_id")


# ═══════════════════════════════════════════════════════════════════════════
#  Phases (V3) — portal-wide + per-project
# ═══════════════════════════════════════════════════════════════════════════
def sync_phases_portal(configuration, state, portal_id):
    url = build_url(configuration, portal_id, "/phases", version="v3")
    try:
        records = list(paginate_v3(configuration, url, "phases",
                                    per_page=200))
    except (ScopeMissing, Exception) as e:
        log.fine(f"phases ({portal_id}): {e!r}")
        return
    for p in records:
        if not isinstance(p, dict):
            continue
        pid = p.get("id") or p.get("phase_id") or p.get("id_string")
        if pid is None:
            continue
        flat = flatten_record(p)
        flat["portal_id"] = portal_id
        flat["phase_id"] = str(pid)
        flat["project_id"] = None
        flat["scope"] = "portal"
        upsert("phases", flat, id_key="portal_id|phase_id")


def sync_phases_for_project(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/phases", version="v3")
    try:
        records = list(paginate_v3(configuration, url, "phases",
                                    per_page=200))
    except (ScopeMissing, Exception):
        return
    for p in records:
        if not isinstance(p, dict):
            continue
        pid = p.get("id") or p.get("phase_id") or p.get("id_string")
        if pid is None:
            continue
        flat = flatten_record(p)
        flat["portal_id"] = portal_id
        flat["phase_id"] = str(pid)
        flat["project_id"] = str(project_id)
        flat["scope"] = "project"
        upsert("phases", flat, id_key="portal_id|phase_id")


# ═══════════════════════════════════════════════════════════════════════════
#  Teams (V3) — portal-wide + per-project junction
# ═══════════════════════════════════════════════════════════════════════════
def sync_teams_portal(configuration, state, portal_id):
    url = build_url(configuration, portal_id, "/teams", version="v3")
    try:
        records = list(paginate_v3(configuration, url, "teams",
                                    per_page=200))
    except (ScopeMissing, Exception) as e:
        log.fine(f"teams ({portal_id}): {e!r}")
        return
    for t in records:
        if not isinstance(t, dict):
            continue
        tid = t.get("id") or t.get("team_id") or t.get("id_string")
        if tid is None:
            continue
        flat = flatten_record(t)
        flat["portal_id"] = portal_id
        flat["team_id"] = str(tid)
        upsert("teams", flat, id_key="portal_id|team_id")


def sync_project_teams(configuration, state, portal_id, project_id):
    url = build_url(configuration, portal_id,
                    f"/projects/{project_id}/teams", version="v3")
    try:
        records = list(paginate_v3(configuration, url, "teams",
                                    per_page=200))
    except (ScopeMissing, Exception):
        return
    for t in records:
        if not isinstance(t, dict):
            continue
        tid = t.get("id") or t.get("team_id") or t.get("id_string")
        if tid is None:
            continue
        flat = flatten_record(t)
        flat["portal_id"] = portal_id
        flat["project_id"] = str(project_id)
        flat["team_id"] = str(tid)
        upsert("project_teams", flat,
               id_key="portal_id|project_id|team_id")


# ═══════════════════════════════════════════════════════════════════════════
#  Profiles + Roles (V3) — portal-wide dim tables for permissions analytics
# ═══════════════════════════════════════════════════════════════════════════
def sync_profiles(configuration, state, portal_id):
    url = build_url(configuration, portal_id, "/profiles", version="v3")
    try:
        records = list(paginate_v3(configuration, url, "profiles",
                                    per_page=200))
    except (ScopeMissing, Exception) as e:
        log.fine(f"profiles ({portal_id}): {e!r}")
        return
    for p in records:
        if not isinstance(p, dict):
            continue
        pid = p.get("id") or p.get("profile_id") or p.get("id_string")
        if pid is None:
            continue
        flat = flatten_record(p)
        flat["portal_id"] = portal_id
        flat["profile_id"] = str(pid)
        upsert("profiles", flat, id_key="portal_id|profile_id")


def sync_roles(configuration, state, portal_id):
    url = build_url(configuration, portal_id, "/roles", version="v3")
    try:
        records = list(paginate_v3(configuration, url, "roles",
                                    per_page=200))
    except (ScopeMissing, Exception) as e:
        log.fine(f"roles ({portal_id}): {e!r}")
        return
    for r in records:
        if not isinstance(r, dict):
            continue
        rid = r.get("id") or r.get("role_id") or r.get("id_string")
        if rid is None:
            continue
        flat = flatten_record(r)
        flat["portal_id"] = portal_id
        flat["role_id"] = str(rid)
        upsert("roles", flat, id_key="portal_id|role_id")
