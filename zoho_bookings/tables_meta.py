"""
Meta-table sync for the Zoho Bookings connector.

Workspaces, services, staff, and resources are small, slow-moving entities.
Every sync fetches the full current set and emits hard-delete diffs.

Endpoint mapping:
  workspaces → GET /bookings/v1/json/workspaces
  services   → GET /bookings/v1/json/services?workspace_id=X     (per workspace)
  staff      → GET /bookings/v1/json/staffs?workspace_id=X       (per workspace)
  resources  → GET /bookings/v1/json/resources

Array fields on services (`assigned_staffs`, `assigned_groups`) and staff
(`assigned_services`, `assigned_workspaces`) are NOT stored inline on the
parent row — they're emitted as separate bridge-table rows for clean SQL
joins downstream.
"""

from fivetran_connector_sdk import Logging as log

from api_client import api_request
from auth import api_host
from helpers import (
    flatten_record_auto, ids_seen, reconcile_deletes, upsert,
)


SERVICE_ARRAY_KEYS = {"assigned_staffs", "assigned_groups", "assigned_workspaces"}
STAFF_ARRAY_KEYS = {"assigned_services", "assigned_workspaces"}


def _resolve_workspace_ids(config: dict) -> list:
    """Return the list of workspace IDs to sync.

    `workspaces` in config is a comma-separated string of IDs (Fivetran SDK
    requires every config value to be a string — arrays aren't allowed). An
    empty/missing value means "sync every workspace the token can see".
    Lists are also tolerated for backwards-compat with local `validate.py`
    runs where the user might pass a real list.
    """
    raw = config.get("workspaces")
    if isinstance(raw, list):
        configured = [str(w).strip() for w in raw if str(w).strip()]
    elif isinstance(raw, str):
        configured = [w.strip() for w in raw.split(",") if w.strip()]
    else:
        configured = []

    if configured:
        return configured

    url = f"{api_host(config)}/bookings/v1/json/workspaces"
    rv = api_request(config, url, method="GET")
    workspaces = rv.get("data") or []
    return [str(w.get("id")) for w in workspaces if w.get("id")]


def sync_workspaces(config: dict, state: dict, workspace_ids: list):
    """List all workspaces and upsert the ones in `workspace_ids`."""
    url = f"{api_host(config)}/bookings/v1/json/workspaces"
    rv = api_request(config, url, method="GET")
    workspaces = rv.get("data") or []

    wanted = set(str(w) for w in workspace_ids)
    n = 0
    for w in workspaces:
        wid = str(w.get("id") or "")
        if not wid or wid not in wanted:
            continue
        row = flatten_record_auto(w)
        upsert("workspaces", row, id_key="id")
        n += 1
    log.info(f"workspaces: upserted {n} of {len(workspaces)} returned")

    reconcile_deletes("workspaces", ids_seen("workspaces"), state)


def sync_services(config: dict, state: dict, workspace_ids: list):
    """For each workspace, list services. Emit parent rows + bridge rows."""
    url = f"{api_host(config)}/bookings/v1/json/services"
    n_total = 0
    for wid in workspace_ids:
        rv = api_request(config, url, params={"workspace_id": wid}, method="GET")
        services = rv.get("data") or []
        for s in services:
            sid = str(s.get("id") or "")
            if not sid:
                continue
            # Parent row — drop the array fields, they go to bridge tables.
            row = flatten_record_auto(s, drop_keys=SERVICE_ARRAY_KEYS)
            upsert("services", row, id_key="id")
            n_total += 1

            # Bridge: service ↔ staff
            for staff_id in s.get("assigned_staffs") or []:
                if staff_id is None:
                    continue
                upsert(
                    "service_staff_assignments",
                    {"service_id": sid, "staff_id": str(staff_id)},
                    id_key="service_id|staff_id",
                )

            # Bridge: service ↔ workspace. `assigned_workspace` (singular) is
            # the canonical field per Zoho's response; `assigned_workspaces`
            # (plural) sometimes appears too. Cover both.
            ws_field = s.get("assigned_workspace") or s.get("assigned_workspaces")
            if isinstance(ws_field, list):
                for w in ws_field:
                    if w is None:
                        continue
                    upsert(
                        "service_workspace_assignments",
                        {"service_id": sid, "workspace_id": str(w)},
                        id_key="service_id|workspace_id",
                    )
            elif ws_field:
                upsert(
                    "service_workspace_assignments",
                    {"service_id": sid, "workspace_id": str(ws_field)},
                    id_key="service_id|workspace_id",
                )
            else:
                # Fall back to the workspace we queried under.
                upsert(
                    "service_workspace_assignments",
                    {"service_id": sid, "workspace_id": wid},
                    id_key="service_id|workspace_id",
                )

    log.info(f"services: upserted {n_total} across {len(workspace_ids)} workspace(s)")
    reconcile_deletes("services", ids_seen("services"), state)
    reconcile_deletes(
        "service_staff_assignments", ids_seen("service_staff_assignments"), state,
        key_template={"service_id": 0, "staff_id": 1},
    )
    reconcile_deletes(
        "service_workspace_assignments", ids_seen("service_workspace_assignments"), state,
        key_template={"service_id": 0, "workspace_id": 1},
    )


def sync_staff(config: dict, state: dict, workspace_ids: list):
    """For each workspace, list staff. Emit parent rows + bridge rows."""
    url = f"{api_host(config)}/bookings/v1/json/staffs"
    n_total = 0
    for wid in workspace_ids:
        rv = api_request(config, url, params={"workspace_id": wid}, method="GET")
        staff_list = rv.get("data") or []
        for st in staff_list:
            stid = str(st.get("id") or "")
            if not stid:
                continue
            row = flatten_record_auto(st, drop_keys=STAFF_ARRAY_KEYS)
            upsert("staff", row, id_key="id")
            n_total += 1

            for svc_id in st.get("assigned_services") or []:
                if svc_id is None:
                    continue
                upsert(
                    "staff_service_assignments",
                    {"staff_id": stid, "service_id": str(svc_id)},
                    id_key="staff_id|service_id",
                )

            ws_list = st.get("assigned_workspaces") or []
            if not ws_list:
                # Fall back to the workspace we queried under.
                ws_list = [wid]
            for w in ws_list:
                if w is None:
                    continue
                upsert(
                    "staff_workspace_assignments",
                    {"staff_id": stid, "workspace_id": str(w)},
                    id_key="staff_id|workspace_id",
                )

    log.info(f"staff: upserted {n_total} across {len(workspace_ids)} workspace(s)")
    reconcile_deletes("staff", ids_seen("staff"), state)
    reconcile_deletes(
        "staff_service_assignments", ids_seen("staff_service_assignments"), state,
        key_template={"staff_id": 0, "service_id": 1},
    )
    reconcile_deletes(
        "staff_workspace_assignments", ids_seen("staff_workspace_assignments"), state,
        key_template={"staff_id": 0, "workspace_id": 1},
    )


def sync_resources(config: dict, state: dict):
    """List all resources (no workspace param)."""
    url = f"{api_host(config)}/bookings/v1/json/resources"
    rv = api_request(config, url, method="GET")
    resources = rv.get("data") or []
    for r in resources:
        rid = str(r.get("id") or "")
        if not rid:
            continue
        row = flatten_record_auto(r)
        upsert("resources", row, id_key="id")
    log.info(f"resources: upserted {len(resources)}")
    reconcile_deletes("resources", ids_seen("resources"), state)


def sync_meta_all(config: dict, state: dict) -> list:
    """Orchestrator. Returns the resolved workspace ID list so the data-table
    layer doesn't have to re-call /workspaces."""
    workspace_ids = _resolve_workspace_ids(config)
    log.info(f"Resolved workspaces to sync: {workspace_ids}")
    sync_workspaces(config, state, workspace_ids)
    sync_services(config, state, workspace_ids)
    sync_staff(config, state, workspace_ids)
    sync_resources(config, state)
    return workspace_ids
