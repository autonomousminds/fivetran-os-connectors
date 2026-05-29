"""
Sync logic for Xero Projects API.

Projects API uses page/pageSize pagination (max 500 per page) and supports
full sync only (no If-Modified-Since equivalent at endpoint level — there
is a 'states' filter but no incremental cursor).
"""

from fivetran_connector_sdk import Logging as log

from api_client import PROJECTS_BASE, api_request
from helpers import upsert as _upsert


def _paginate(config, endpoint: str, key: str = "items"):
    """Generator over all pages of a Projects-API list endpoint."""
    page = 1
    while True:
        url = f"{PROJECTS_BASE}{endpoint}"
        data = api_request(config, url,
                           params={"page": page, "pageSize": 500},
                           scope_group="projects")
        # Projects API uses 'items' (lowercase) as the wrapper key.
        items = data.get(key, []) or data.get("Items", []) or []
        if not items:
            return
        yield from items
        pagination = data.get("pagination", {}) or {}
        total_pages = pagination.get("pageCount") or pagination.get("pageSize")
        if total_pages and page >= total_pages:
            return
        if len(items) < 500:
            return
        page += 1


def sync_projects(config, state):
    count = 0
    project_ids = []
    for p in _paginate(config, "/Projects"):
        pid = p.get("projectId") or p.get("ProjectId", "")
        if not pid:
            continue
        project_ids.append(pid)
        _upsert("projects_project", {
            "ProjectId":           pid,
            "ContactId":           p.get("contactId", ""),
            "Name":                p.get("name", ""),
            "Status":              p.get("status", ""),
            "DeadlineUtc":         p.get("deadlineUtc", ""),
            "EstimateAmount":      (p.get("estimate") or {}).get("value"),
            "TotalInvoiced":       (p.get("totalInvoiced") or {}).get("value"),
            "TotalToBeInvoiced":   (p.get("totalToBeInvoiced") or {}).get("value"),
            "TaskAmount":          (p.get("taskAmount") or {}).get("value"),
            "ExpenseAmount":       (p.get("expenseAmount") or {}).get("value"),
            "MinutesLogged":       p.get("minutesLogged"),
            "MinutesToBeInvoiced": p.get("minutesToBeInvoiced"),
            "Currency":            p.get("currencyCode", ""),
            "IsTracked":           p.get("isTracked", False),
        })
        count += 1
    log.info(f"Projects: {count} synced")
    state["_project_ids"] = project_ids  # for downstream task/time syncs


def sync_tasks(config, state):
    project_ids = state.get("_project_ids", []) or []
    count = 0
    for pid in project_ids:
        for t in _paginate(config, f"/Projects/{pid}/Tasks"):
            _upsert("projects_task", {
                "ProjectId":       pid,
                "TaskId":          t.get("taskId") or t.get("TaskId", ""),
                "Name":            t.get("name", ""),
                "Status":          t.get("status", ""),
                "Rate":            (t.get("rate") or {}).get("value"),
                "ChargeType":      t.get("chargeType", ""),
                "EstimateMinutes": t.get("estimateMinutes"),
                "TotalMinutes":    t.get("totalMinutes"),
                "TotalAmount":     (t.get("totalAmount") or {}).get("value"),
                "MinutesInvoiced": t.get("minutesInvoiced"),
                "MinutesToBeInvoiced": t.get("minutesToBeInvoiced"),
            })
            count += 1
    log.info(f"Tasks: {count} synced")


def sync_time_entries(config, state):
    project_ids = state.get("_project_ids", []) or []
    count = 0
    for pid in project_ids:
        for te in _paginate(config, f"/Projects/{pid}/Time"):
            _upsert("projects_time_entry", {
                "TimeEntryId": te.get("timeEntryId") or te.get("TimeEntryId", ""),
                "ProjectId":   pid,
                "TaskId":      te.get("taskId", ""),
                "UserId":      te.get("userId", ""),
                "Duration":    te.get("duration"),
                "DateUtc":     te.get("dateUtc", ""),
                "Description": te.get("description", ""),
                "Status":      te.get("status", ""),
            })
            count += 1
    log.info(f"Time entries: {count} synced")


def sync_project_users(config, state):
    count = 0
    for u in _paginate(config, "/projectsusers"):
        _upsert("projects_project_user", {
            "UserId": u.get("userId") or u.get("UserId", ""),
            "Name":   u.get("name", ""),
            "Email":  u.get("email", ""),
        })
        count += 1
    log.info(f"Project users: {count} synced")


PROJECTS_SYNCS = [sync_project_users, sync_projects, sync_tasks, sync_time_entries]
