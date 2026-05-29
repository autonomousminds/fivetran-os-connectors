"""
Transactional / high-volume tables for the Zoho People connector.

Six logical groups:

  1. **Per-form records** — one data table per form discovered. Driven by
     the form's default view via `/api/forms/{view}/records`, the only
     endpoint that supports the `modifiedtime` incremental filter.

  2. **Attendance daily** — org-wide per-employee daily summary, sourced
     from `/people/api/attendance/getUserReport` bulk variant. Backfills
     a configurable past window; the cursor is "max date seen so far".

  3. **Leave** — `leave_records` (paginated org-wide), `leave_balance`
     (per-employee booked + balance per period), `leave_types` derived
     from records seen.

  4. **Timetracker** — `jobs` (paginated) and `timelogs` (month-by-month
     windows because Zoho caps a single timelog query at 1 month).

  5. **LMS courses** — optional, gated on `sync_lms_courses` config.

Strategy notes:
  - Form records: incremental via `modifiedtime` (ms since epoch). First
    run does a full sync (no cursor); subsequent runs filter. Every 7
    days we re-run a full sync to catch hard-deletes (the modifiedtime
    filter never returns deleted IDs).
  - Attendance / leave / timelogs: paged by date windows. No reliable
    "modified since" filter on these endpoints, so we re-pull the
    configured past window every run (records are immutable once posted,
    so this is idempotent).
"""

import json
import time
from datetime import datetime, timedelta, timezone  # noqa: F401  (used below)

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import (
    DailyLimitExceeded,
    ScopeMissing,
    api_request,
)
from auth import api_host
from helpers import (
    config_bool,
    config_int,
    extract_subforms,
    flatten_record_auto,
    ids_seen,
    reconcile_deletes,
    upsert,
)
from schema import child_table_for_subform, discover


CHECKPOINT_EVERY = 500
FULL_SYNC_INTERVAL_SECONDS = 7 * 24 * 3600


# Cross-function leave-type dedup. Both `sync_leave_records` and
# `sync_leave_balance` discover leave types as a side effect — calling
# upsert N times for the same ID is wasted budget. Reset at the start
# of every sync run via reset_dedup_caches() (called from connector.py).
_seen_leave_type_ids: set = set()

# Cached employee list for per-employee endpoints (attendance_entries,
# shift_mappings, learner_progress). Populated by `_discover_employees`
# once per run and reused by the per-employee syncs.
_employees: list = []
_employees_discovered: list = [False]


def reset_dedup_caches():
    _seen_leave_type_ids.clear()
    _employees.clear()
    _employees_discovered[0] = False


def _discover_employees(configuration: dict) -> list:
    """Fetch the employee list from `/api/forms/P_EmployeeView/records` and
    return tuples of {employee_id, email_id, erecno, first_name, last_name}.
    Cached at module level — only fetches on first call per run.

    Used by per-employee endpoints (attendance_entries, shift_mappings,
    learner_progress) so we don't bombard the forms endpoint repeatedly."""
    if _employees_discovered[0]:
        return _employees

    host = api_host(configuration)
    url = f"{host}/api/forms/P_EmployeeView/records"

    cur = 1
    page_size = 200
    n_pages = 0
    while True:
        params = {"sIndex": cur, "rec_limit": page_size}
        try:
            body = api_request(configuration, url, params=params)
        except ScopeMissing as e:
            log.warning(f"_discover_employees: scope missing, returning empty list. {e!s}")
            _employees_discovered[0] = True
            return _employees

        records = body if isinstance(body, list) else (
            body.get("response", {}).get("result", []) if isinstance(body, dict) else []
        )
        if not isinstance(records, list) or not records:
            break

        # Stop on the empty-form sentinel (single error-envelope record).
        if _looks_like_empty_sentinel(records):
            break

        n_pages += 1
        for r in records:
            if not isinstance(r, dict):
                continue
            emp = {
                "employee_id": (r.get("EmployeeID") or r.get("Employee ID")
                                or r.get("employeeId")),
                "email_id":    (r.get("EmailID") or r.get("Email address")
                                or r.get("emailId") or r.get("Email ID")),
                "erecno":      r.get("Erecno") or r.get("erecno") or r.get("recordId"),
                "first_name":  (r.get("FirstName") or r.get("First Name")
                                or r.get("firstName")),
                "last_name":   (r.get("LastName") or r.get("Last Name")
                                or r.get("lastName")),
            }
            # Use whichever identifier is non-empty for the API calls.
            if any(emp.values()):
                _employees.append(emp)

        if len(records) < page_size:
            break
        cur += page_size

    _employees_discovered[0] = True
    log.info(f"_discover_employees: cached {len(_employees)} employee(s) "
             f"across {n_pages} page(s)")
    return _employees


def _maybe_upsert_leave_type(lt_id, name=None, unit=None, type_=None):
    """Upsert exactly once per leave_type_id per sync run."""
    if lt_id is None:
        return
    key = str(lt_id)
    if key in _seen_leave_type_ids:
        return
    _seen_leave_type_ids.add(key)
    upsert("leave_types", {
        "id":   key,
        "name": name,
        "unit": unit,
        "type": type_,
    }, id_key="id")


# ═══════════════════════════════════════════════════════════════════════════
#  Per-form record sync
# ═══════════════════════════════════════════════════════════════════════════
def _should_full_sync(state: dict, table: str) -> bool:
    last = state.get(f"{table}__last_full_sync")
    if not last:
        return True
    return (time.time() - float(last)) > FULL_SYNC_INTERVAL_SECONDS


def _mark_full_sync(state: dict, table: str):
    state[f"{table}__last_full_sync"] = time.time()


def _record_id(record: dict) -> str:
    """Zoho People uses different keys across endpoints — normalise."""
    for k in ("recordId", "Zoho_ID", "ZohoID", "id", "ID", "EntityId"):
        v = record.get(k)
        if v is not None:
            return str(v)
    return None


_ERROR_SENTINEL_KEYS = {"errorcode", "errorCode", "error_code",
                        "errors", "Errors", "error",
                        "message", "Message"}
_VALID_RECORD_KEYS = {"recordId", "Zoho_ID", "ZohoID", "id", "ID",
                      "EntityId", "Erecno", "erecno"}


def _looks_like_empty_sentinel(records: list) -> bool:
    """Detect Zoho's empty-form sentinel: a 1-element array containing only
    error/status fields (no recordId, no Zoho_ID).

    Examples returned for empty form views:
        [{"errorcode": 7300, "message": "No records found"}]
        [{"error": "...", "message": "..."}]
    """
    if not isinstance(records, list) or len(records) != 1:
        return False
    r = records[0]
    if not isinstance(r, dict) or not r:
        return False
    if any(k in r for k in _VALID_RECORD_KEYS):
        return False
    return any(k in r for k in _ERROR_SENTINEL_KEYS)


def _process_form_record(form: dict, raw: dict, current_ids: set) -> bool:
    """Flatten the parent record, emit subform child rows. Returns True if
    a row was actually upserted, False if the record was an error sentinel
    or otherwise skipped.

    Form records from `/api/forms/{view}/records` use display-name keys
    (`"First Name"`, `"Employee ID"`) — we sanitise these into snake_case
    via `flatten_record_auto`. Any list-of-dicts field is treated as a
    tabular section → child table named
    `{form_table}__sub_{safe(field)}`."""
    table = form["table"]
    form_link = form["form_link_name"]

    # Reject single error envelopes that snuck past the bulk filter.
    if not any(k in raw for k in _VALID_RECORD_KEYS) and \
            any(k in raw for k in _ERROR_SENTINEL_KEYS):
        log.fine(f"  {table}: skipping error-envelope row: {raw}")
        return False

    rid = _record_id(raw)
    subforms = extract_subforms(raw)

    flat = flatten_record_auto(raw, drop_keys=set(subforms.keys()))
    if rid is not None:
        flat["record_id"] = rid
    if not flat.get("record_id"):
        log.fine(f"  {table}: skipping record with no recordId — keys={list(raw.keys())[:10]}")
        return False

    upsert(table, flat, id_key="record_id")
    current_ids.add(flat["record_id"])

    for sub_field, sub_rows in subforms.items():
        child_table = child_table_for_subform(form_link, sub_field)
        for idx, child_raw in enumerate(sub_rows):
            child_flat = flatten_record_auto(child_raw)
            child_flat["parent_record_id"] = flat["record_id"]
            child_id = _record_id(child_raw)
            if child_id is None:
                child_id = f"{flat['record_id']}__{idx}"
            child_flat["record_id"] = child_id
            upsert(child_table, child_flat, id_key="record_id")
    return True


def _fetch_form_records(configuration: dict, view_name: str,
                       modifiedtime_ms: int = None):
    """Generator over `/api/forms/{view}/records`.

    Pagination: `sIndex=1, rec_limit=200` (Zoho's largest documented size).
    sIndex is 1-indexed and represents "starting record number" (not
    offset), so the next page is sIndex + rec_limit.

    `modifiedtime` is in milliseconds since epoch; pass None for full
    sync.

    Response shape: top-level array of record dicts, no envelope.
    """
    host = api_host(configuration)
    url = f"{host}/api/forms/{view_name}/records"

    page_size = 200
    cur = 1
    page = 0
    yielded = 0
    while True:
        params = {"sIndex": cur, "rec_limit": page_size}
        if modifiedtime_ms is not None:
            params["modifiedtime"] = modifiedtime_ms

        body = api_request(configuration, url, params=params)

        # Two response shapes seen in the wild:
        #   - bare list of records (newer)
        #   - {"response": {"result": [...]}} (some module variations)
        records = None
        if isinstance(body, list):
            records = body
        elif isinstance(body, dict):
            inner = body.get("response", body)
            if isinstance(inner, dict):
                cand = inner.get("result")
                if isinstance(cand, list):
                    records = cand
            elif isinstance(inner, list):
                records = inner

        if records is None:
            log.fine(f"  {view_name} page {page+1}: empty/non-list body, stopping. "
                     f"Body snippet: {str(body)[:200]}")
            return

        # Empty-form sentinel: Zoho returns [{"errorcode":..., "message":...}]
        # instead of [] when a form view has no rows. Treat as empty.
        if _looks_like_empty_sentinel(records):
            log.fine(f"  {view_name} page {page+1}: empty-form sentinel — stopping. "
                     f"{records[0]}")
            return

        page += 1
        for r in records:
            if isinstance(r, dict):
                yield r
                yielded += 1
        log.fine(f"  {view_name} page {page}: {len(records)} record(s) (total {yielded})")
        if len(records) < page_size:
            return
        cur += page_size


def _sync_one_form(configuration: dict, state: dict, form: dict):
    """Sync records for a single form. Picks full or incremental based on
    state + the 7-day full-sync schedule."""
    table = form["table"]
    view_name = form.get("default_view")
    if not view_name:
        log.warning(f"{table}: no default view discovered — skipping")
        return

    cursor_key = f"{table}__last_modified_ms"
    last_modified_ms = state.get(cursor_key)
    full_mode = _should_full_sync(state, table) or not last_modified_ms

    mode_str = "FULL" if full_mode else f"INCREMENTAL (since={last_modified_ms}ms)"
    log.info(f"{table} [{view_name}]: {mode_str}")

    current_ids: set = set()
    max_modified_ms = last_modified_ms or 0
    n = 0
    try:
        gen = _fetch_form_records(configuration, view_name,
                                  modifiedtime_ms=(None if full_mode else last_modified_ms))
        for raw in gen:
            wrote = _process_form_record(form, raw, current_ids)
            if not wrote:
                continue
            # Pull the modified-time millisecond field if present.
            for k in ("ModifiedTime", "modifiedTime", "Modified Time",
                      "modifiedtime", "Last Modified Time",
                      "lastModifiedTime", "LastModifiedTime"):
                mv = raw.get(k)
                if isinstance(mv, (int, float)) and mv > max_modified_ms:
                    max_modified_ms = int(mv)
                    break
                if isinstance(mv, str) and mv.isdigit():
                    iv = int(mv)
                    if iv > max_modified_ms:
                        max_modified_ms = iv
                        break
            n += 1
            if n % CHECKPOINT_EVERY == 0:
                if max_modified_ms:
                    state[cursor_key] = max_modified_ms
                op.checkpoint(state=state)
                log.fine(f"  {table}: checkpoint at {n} row(s)")
    except ScopeMissing as e:
        log.warning(f"{table}: scope missing — skipping. Detail: {e!s}")
        return
    except Exception as e:
        log.severe(f"{table}: failed with {e!r}. Continuing with next form.")
        return

    if max_modified_ms:
        state[cursor_key] = max_modified_ms
    if full_mode:
        reconcile_deletes(table, current_ids, state, key_template="record_id")
        _mark_full_sync(state, table)
    op.checkpoint(state=state)
    log.info(f"{table}: synced {n} record(s), cursor={max_modified_ms}ms")


def sync_all_forms(configuration: dict, state: dict):
    """Iterate every form in the discovery catalog."""
    catalog = discover(configuration)
    forms = catalog.get("forms", [])
    log.info(f"sync_all_forms: {len(forms)} form(s) to process")
    for form in forms:
        try:
            _sync_one_form(configuration, state, form)
        except DailyLimitExceeded:
            raise
        except Exception as e:
            log.severe(f"{form['table']}: outer failure {e!r}, continuing")


# ═══════════════════════════════════════════════════════════════════════════
#  Attendance — daily org-wide
# ═══════════════════════════════════════════════════════════════════════════
def sync_attendance_daily(configuration: dict, state: dict):
    """`GET /people/api/attendance/getUserReport?sdate=&edate=&dateFormat=yyyy-MM-dd&startIndex=`.

    Bulk org-wide variant. Each call returns up to 100 employees, each with
    `attendanceDetails` (date-keyed daily summary) and `employeeDetails`.
    Pagination: `startIndex=0, 100, 200, ...` (0-indexed). Stop when fewer
    than 100 employees come back.

    We re-pull the full configured past window every run — attendance
    records can be edited (regularised) after the fact, so a fresh pull
    keeps the table current. Idempotent (PK = employee_id + date).
    """
    past_days = config_int(configuration, "attendance_past_window_days", 180)
    now = datetime.now(timezone.utc).date()
    sdate = (now - timedelta(days=past_days)).strftime("%Y-%m-%d")
    edate = now.strftime("%Y-%m-%d")

    host = api_host(configuration)
    url = f"{host}/people/api/attendance/getUserReport"

    log.info(f"attendance_daily: pulling window [{sdate} → {edate}]")
    n_rows = 0
    n_employees = 0
    start_index = 0
    page = 0
    while True:
        params = {
            "sdate":       sdate,
            "edate":       edate,
            "dateFormat":  "yyyy-MM-dd",
            "startIndex":  start_index,
        }
        try:
            body = api_request(configuration, url, params=params)
        except ScopeMissing as e:
            log.warning(f"attendance_daily: scope missing — skipping. {e!s}")
            return
        except Exception as e:
            log.warning(f"attendance_daily: page failed {e!r}, stopping")
            return

        # Response shape: the bulk variant returns
        #   {"response": {"result": [<emp1>, <emp2>, ...]}}
        # where each <emp> has keys `attendanceDetails` and `employeeDetails`.
        # Some tenants flatten the employee record up one level.
        inner = body.get("response", body) if isinstance(body, dict) else {}
        result = inner.get("result") if isinstance(inner, dict) else None
        if not isinstance(result, list):
            log.fine(f"attendance_daily page {page+1}: empty result")
            break

        page += 1
        if not result:
            break

        for emp_record in result:
            if not isinstance(emp_record, dict):
                continue
            emp_details = emp_record.get("employeeDetails", emp_record)
            attendance = emp_record.get("attendanceDetails") or {}
            emp_id = (emp_details.get("EmployeeID") or emp_details.get("employeeId")
                      or emp_details.get("erecno") or emp_details.get("Erecno"))
            emp_mail = (emp_details.get("EmployeeMailID") or emp_details.get("emailId")
                        or emp_details.get("EmailID"))
            emp_first = emp_details.get("FirstName") or emp_details.get("firstName")
            emp_last = emp_details.get("LastName") or emp_details.get("lastName")
            erecno = (emp_details.get("Erecno") or emp_details.get("erecno")
                      or emp_details.get("zuid"))

            if isinstance(attendance, dict):
                for date_str, day in attendance.items():
                    if not isinstance(day, dict):
                        continue
                    row = {
                        "employee_id":       str(emp_id) if emp_id is not None else None,
                        "date":              date_str,
                        "erecno":            str(erecno) if erecno is not None else None,
                        "employee_email":    emp_mail,
                        "employee_first_name": emp_first,
                        "employee_last_name":  emp_last,
                        "working_hours":     day.get("WorkingHours"),
                        "total_hours":       day.get("TotalHours") or day.get("totalHrs"),
                        "over_time":         day.get("OverTime"),
                        "deviation_time":    day.get("DeviationTime"),
                        "status":            day.get("Status"),
                        "shift_name":        day.get("ShiftName"),
                        "first_in":          day.get("FirstIn") or day.get("firstIn"),
                        "last_out":          day.get("LastOut") or day.get("lastOut"),
                        "paid_break":        day.get("paidBreak"),
                        "unpaid_break":      day.get("unPaidBreak"),
                        "raw_day":           json.dumps(day),
                    }
                    if not row["employee_id"] or not row["date"]:
                        continue
                    upsert("attendance_daily", row,
                           id_key="employee_id|date")
                    n_rows += 1
                    if n_rows % CHECKPOINT_EVERY == 0:
                        op.checkpoint(state=state)
            n_employees += 1

        log.fine(f"attendance_daily page {page}: {len(result)} employees, "
                 f"running totals: emps={n_employees}, rows={n_rows}")
        if len(result) < 100:
            break
        start_index += 100

    log.info(f"attendance_daily: {n_employees} employee(s), {n_rows} day-row(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  Leave — records, balance, types
# ═══════════════════════════════════════════════════════════════════════════
def _emit_leave_record(raw: dict, state_ledger: dict):
    """Flatten one leave record into `leave_records` + per-day rows into
    `leave_records_days`.

    Actual response shape from /v2/leavetracker/leaves/records (observed,
    not what the public docs claim):

        {"records": {"<leave_id>": {
            "Zoho.ID":         11111111111111111,
            "From":            "2026-03-18",
            "To":              "2026-03-18",
            "Leavetype.ID":    22222222222222222,
            "Leavetype":       "Annual leave 2026",
            "Unit":            "Days",
            "Type":            "PAID",
            "ApprovalStatus":  "Approved",
            "Reason":          "...",
            "Employee":        "Jane Doe",
            "EmployeeId":      "Jane Doe ",   <- yes, with trailing space
            "TeamEmailID":     "jd@example.com",
            "EmployeePhoto":   "viewPhoto?filename=...",
            "Days": {"2026-03-18": {"LeaveCount": "1.0",
                                     "StartTime": "09:00",
                                     "EndTime":   "18:00"}}
        }, ...}}
    """
    if not isinstance(raw, dict):
        return
    lid = (raw.get("Zoho.ID") or raw.get("recordId") or raw.get("LeaveID")
           or raw.get("leaveId") or raw.get("id"))
    if lid is None:
        return
    lid_str = str(lid)

    lt_id = (raw.get("Leavetype.ID") or raw.get("LeaveType.ID")
             or raw.get("leaveTypeId"))
    lt_name = (raw.get("Leavetype") or raw.get("LeaveType")
               or raw.get("leaveTypeName"))
    lt_unit = raw.get("Unit") or raw.get("unit")
    lt_type = raw.get("Type") or raw.get("type")  # PAID / UNPAID

    # Employee mapping. Zoho returns the display name as both Employee and
    # EmployeeId (the latter often with a trailing space) — no numeric ID.
    emp_name = raw.get("Employee") or raw.get("employee")
    emp_id_field = raw.get("EmployeeId") or raw.get("employeeId")
    if isinstance(emp_id_field, str):
        emp_id_field = emp_id_field.strip()
    emp_email = (raw.get("TeamEmailID") or raw.get("EmailID")
                 or raw.get("Email") or raw.get("emailId"))

    row = {
        "id":               lid_str,
        "leave_type_id":    str(lt_id) if lt_id is not None else None,
        "leave_type_name":  lt_name,
        "unit":             lt_unit,
        "type":             lt_type,
        "approval_status":  raw.get("ApprovalStatus") or raw.get("approvalStatus"),
        "employee_name":    emp_name,
        "employee_id_name": emp_id_field,
        "employee_email":   emp_email,
        "from_date":        raw.get("From") or raw.get("from") or raw.get("fromDate"),
        "to_date":          raw.get("To") or raw.get("to") or raw.get("toDate"),
        "reason":           raw.get("Reason") or raw.get("reason"),
        "applied_date":     raw.get("AppliedDate") or raw.get("appliedDate"),
        "applied_by":       raw.get("AppliedBy") or raw.get("appliedBy"),
        "employee_photo":   raw.get("EmployeePhoto"),
        "raw_record":       json.dumps(raw),
    }
    upsert("leave_records", row, id_key="id")

    # Per-day breakdown — emit into leave_records_days child table.
    days = raw.get("Days") or raw.get("days")
    if isinstance(days, dict):
        for day_date, day_meta in days.items():
            if not isinstance(day_meta, dict):
                continue
            upsert("leave_records_days", {
                "leave_id":    lid_str,
                "date":        day_date,
                "leave_count": day_meta.get("LeaveCount") or day_meta.get("leaveCount"),
                "start_time":  day_meta.get("StartTime") or day_meta.get("startTime"),
                "end_time":    day_meta.get("EndTime") or day_meta.get("endTime"),
                "raw_meta":    json.dumps(day_meta),
            }, id_key="leave_id|date")

    _maybe_upsert_leave_type(lt_id, name=lt_name, unit=lt_unit, type_=lt_type)
    if lt_id is not None:
        state_ledger["leave_type_ids"].add(str(lt_id))


def sync_leave_records(configuration: dict, state: dict):
    """`GET /people/api/v2/leavetracker/leaves/records?from=&to=&startIndex=&limit=200&dateFormat=yyyy-MM-dd`.

    Paginated org-wide endpoint with a **hard 1-year window cap** per call
    (server rejects with 500 + `{"error":{"code":9001,"message":"Date
    period should be within 1 year"}}`). We walk the configured
    [-past, +future] range in ≤364-day chunks (one day under the cap so
    we never hit a boundary error) and paginate each chunk separately.

    Documented rate: 300 requests / 5 min — well above our per-endpoint
    floor of 25, so chunking is safe."""
    past_days = config_int(configuration, "leave_past_window_days", 365)
    future_days = config_int(configuration, "leave_future_window_days", 365)
    now = datetime.now(timezone.utc).date()
    earliest = now - timedelta(days=past_days)
    latest = now + timedelta(days=future_days)

    host = api_host(configuration)
    url = f"{host}/people/api/v2/leavetracker/leaves/records"

    log.info(f"leave_records: walking [{earliest} → {latest}] in ≤1-year chunks")
    state_ledger = {"leave_type_ids": set()}

    n_total = 0
    chunks = 0
    first_body_snippet = None
    chunk_start = earliest
    while chunk_start <= latest:
        chunk_end = min(latest, chunk_start + timedelta(days=364))
        chunks += 1
        from_date = chunk_start.strftime("%Y-%m-%d")
        to_date = chunk_end.strftime("%Y-%m-%d")

        page_size = 200
        cur = 0
        page = 0
        chunk_n = 0
        while True:
            params = {
                "from":       from_date,
                "to":         to_date,
                "startIndex": cur,
                "limit":      page_size,
                "dateFormat": "yyyy-MM-dd",
                # ALL = every employee's leaves (org-wide). Default is
                # MINE (just the OAuth user's own) — useless for BI.
                # Valid: MINE, SUB, DIRSUBS, SUBS, ALL.
                "dataSelect": "ALL",
            }
            try:
                body = api_request(configuration, url, params=params)
            except ScopeMissing as e:
                log.warning(f"leave_records: scope missing — skipping. {e!s}")
                return

            if first_body_snippet is None:
                first_body_snippet = str(body)[:500]

            # Actual response shape: `{"records": {<leave_id>: {<fields>}, ...},
            # "message": "...", "uri": "...", "status": ...}` — NOT the
            # documented `{response: {result: [...]}}`. Handle both.
            records_iter = []
            if isinstance(body, dict):
                # New shape (observed): top-level `records` dict.
                rec_dict = body.get("records")
                if isinstance(rec_dict, dict):
                    records_iter = list(rec_dict.values())
                elif isinstance(rec_dict, list):
                    records_iter = rec_dict
                else:
                    # Fallback to documented shape just in case.
                    inner = body.get("response", body)
                    if isinstance(inner, dict):
                        cand = inner.get("result") or inner.get("records") or []
                        if isinstance(cand, list):
                            records_iter = cand
                        elif isinstance(cand, dict):
                            records_iter = list(cand.values())

            page += 1
            if not records_iter:
                break
            for r in records_iter:
                _emit_leave_record(r, state_ledger)
                n_total += 1
                chunk_n += 1
                if n_total % CHECKPOINT_EVERY == 0:
                    op.checkpoint(state=state)

            log.fine(f"  leave_records chunk #{chunks} [{from_date}→{to_date}] "
                     f"page {page}: {len(records_iter)} (chunk_total {chunk_n}, "
                     f"overall {n_total})")
            if len(records_iter) < page_size:
                break
            cur += page_size

        chunk_start = chunk_end + timedelta(days=1)

    if n_total == 0:
        log.warning(f"leave_records: 0 rows. First-call response snippet: "
                    f"{first_body_snippet}")
    log.info(f"leave_records: {n_total} record(s) across {chunks} year-chunk(s), "
             f"{len(state_ledger['leave_type_ids'])} distinct leave type(s)")


def sync_leave_balance(configuration: dict, state: dict):
    """`GET /people/api/v2/leavetracker/reports/bookedAndBalance?from=&to=&unit=Day&startIndex=&limit=30`.

    Per-employee, per-leave-type booked + balance snapshot. The API caps
    pagination at 30 results per page. Each "row" we emit is one
    (employee, leave_type) intersection for the configured window.

    PK: (employee_id, leave_type_id, from_date, to_date). The window key
    keeps history when the user re-runs with a different window — without
    it, the next window's rows would overwrite the previous."""
    past_days = config_int(configuration, "leave_past_window_days", 365)
    now = datetime.now(timezone.utc).date()
    from_date = (now - timedelta(days=past_days)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")

    host = api_host(configuration)
    url = f"{host}/people/api/v2/leavetracker/reports/bookedAndBalance"

    log.info(f"leave_balance: window [{from_date} → {to_date}]")

    page_size = 30
    cur = 0
    page = 0
    n_total = 0
    while True:
        params = {
            "from":       from_date,
            "to":         to_date,
            "unit":       "Day",
            "dateFormat": "yyyy-MM-dd",
            "startIndex": cur,
            "limit":      page_size,
        }
        try:
            body = api_request(configuration, url, params=params)
        except ScopeMissing as e:
            log.warning(f"leave_balance: scope missing — skipping. {e!s}")
            return

        # Shape per docs:
        #   {"leavetypes": {<lt_id>: {name, unit, type}},
        #    "report":     {<emp_id>: {<lt_id>: {booked, balance, ...}}},
        #    "employees":  [<emp_id>, ...]}
        leavetypes = body.get("leavetypes") or {}
        report = body.get("report") or {}
        employees = body.get("employees") or []

        page += 1
        if not employees and not report:
            break

        # Pre-emit leave_types from this report for completeness.
        # `_maybe_upsert_leave_type` dedupes across the whole run.
        if isinstance(leavetypes, dict):
            for ltid, meta in leavetypes.items():
                if not isinstance(meta, dict):
                    continue
                _maybe_upsert_leave_type(ltid,
                                         name=meta.get("name"),
                                         unit=meta.get("unit"),
                                         type_=meta.get("type"))

        for emp_id in employees:
            emp_buckets = report.get(emp_id) if isinstance(report, dict) else None
            if not isinstance(emp_buckets, dict):
                continue
            for lt_id, vals in emp_buckets.items():
                if not isinstance(vals, dict):
                    continue
                # Skip non-leave-type rollups like "totals" — they aren't UUIDs.
                if not str(lt_id).isdigit() and not str(lt_id).startswith("ZP"):
                    # Still useful as an aggregate row; encode it anyway.
                    pass
                row = {
                    "employee_id":   str(emp_id),
                    "leave_type_id": str(lt_id),
                    "from_date":     from_date,
                    "to_date":       to_date,
                    "booked":        vals.get("booked"),
                    "balance":       vals.get("balance"),
                    "available":     vals.get("available"),
                    "unit":          vals.get("unit") or "Day",
                    "raw_vals":      json.dumps(vals),
                }
                upsert("leave_balance", row,
                       id_key="employee_id|leave_type_id|from_date|to_date")
                n_total += 1

        if len(employees) < page_size:
            break
        cur += page_size
        if n_total % CHECKPOINT_EVERY == 0:
            op.checkpoint(state=state)

    log.info(f"leave_balance: {n_total} (employee, leave_type) snapshot(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  Time tracker — jobs + timelogs
# ═══════════════════════════════════════════════════════════════════════════
def sync_jobs(configuration: dict, state: dict):
    """`GET /people/api/timetracker/getjobs?assignedTo=all&sIndex=0&limit=200`.

    Tight rate limit (20/5min) — small org-wide table though. Sync all
    statuses (in-progress + completed) so BI can see closed projects too.
    """
    host = api_host(configuration)
    url = f"{host}/people/api/timetracker/getjobs"

    log.info("jobs: starting")
    page_size = 200
    cur = 0
    page = 0
    n_total = 0
    first_body_snippet = None
    while True:
        params = {
            "assignedTo":     "all",
            "jobStatus":      "all",
            "isAssigneeCount": "false",
            "fetchLoggedHrs":  "true",
            "sIndex":          cur,
            "limit":           page_size,
        }
        try:
            body = api_request(configuration, url, params=params)
        except ScopeMissing as e:
            log.warning(f"jobs: scope missing — skipping. {e!s}")
            return

        if first_body_snippet is None:
            first_body_snippet = str(body)[:300]

        inner = body.get("response", body) if isinstance(body, dict) else {}
        records = inner.get("result") if isinstance(inner, dict) else []
        if not isinstance(records, list):
            records = []
        is_next = bool(inner.get("isNextAvailable")
                       if isinstance(inner, dict) else False)

        page += 1
        if not records:
            break
        for r in records:
            if not isinstance(r, dict):
                continue
            jid = r.get("jobId") or r.get("JobId") or r.get("id")
            if jid is None:
                continue
            upsert("jobs", {
                "job_id":             str(jid),
                "job_name":           r.get("jobName") or r.get("JobName"),
                "description":        r.get("description"),
                "job_status":         r.get("jobStatus") or r.get("JobStatus"),
                "project_id":         str(r["projectId"]) if r.get("projectId") is not None else None,
                "project_name":       r.get("projectName") or r.get("ProjectName"),
                "client_id":          str(r["clientId"]) if r.get("clientId") is not None else None,
                "client_name":        r.get("clientName") or r.get("ClientName"),
                "from_date":          r.get("fromDate"),
                "to_date":            r.get("toDate"),
                "estimated_hours":    r.get("hours"),
                "logged_hours":       r.get("totalhours"),
                "billable_status":    r.get("jobBillableStatus") or r.get("billingStatus"),
                "rate_per_hour":      r.get("ratePerHour"),
                "owner":              r.get("owner"),
                "assigned_by":        r.get("assignedBy"),
                "raw_record":         json.dumps(r),
            }, id_key="job_id")
            n_total += 1

        log.fine(f"  jobs page {page}: {len(records)} (total {n_total})")
        if not is_next or len(records) < page_size:
            break
        cur += page_size

    if n_total == 0:
        log.warning(f"jobs: 0 rows. First-call response snippet: {first_body_snippet}")
    log.info(f"jobs: {n_total} job(s)")
    reconcile_deletes("jobs", ids_seen("jobs"), state, key_template="job_id")


def sync_timelogs(configuration: dict, state: dict):
    """`GET /people/api/timetracker/gettimelogs`.

    Zoho caps each query at 1 month — we walk the configured past window
    in monthly chunks. Each chunk is paginated separately.

    Per-employee `user=all` returns timelogs across the org. Approval +
    billing filters set to `all` so we get every state."""
    past_days = config_int(configuration, "timelog_past_window_days", 180)
    now = datetime.now(timezone.utc).date()
    earliest = now - timedelta(days=past_days)

    host = api_host(configuration)
    url = f"{host}/people/api/timetracker/gettimelogs"

    log.info(f"timelogs: walking [{earliest} → {now}] in 1-month chunks")
    n_total = 0
    chunk_end = now
    chunks = 0
    first_body_snippet = None
    while chunk_end >= earliest:
        chunk_start = max(earliest, chunk_end - timedelta(days=30))
        chunks += 1
        page_size = 200
        cur = 0
        page = 0
        while True:
            params = {
                "user":           "all",
                "fromDate":       chunk_start.strftime("%Y-%m-%d"),
                "toDate":         chunk_end.strftime("%Y-%m-%d"),
                "dateFormat":     "yyyy-MM-dd",
                "billingStatus":  "all",
                "approvalStatus": "all",
                "sIndex":         cur,
                "limit":          page_size,
            }
            try:
                body = api_request(configuration, url, params=params)
            except ScopeMissing as e:
                log.warning(f"timelogs: scope missing — skipping. {e!s}")
                return

            if first_body_snippet is None:
                first_body_snippet = str(body)[:300]

            inner = body.get("response", body) if isinstance(body, dict) else {}
            records = inner.get("result") if isinstance(inner, dict) else []
            if not isinstance(records, list):
                records = []

            page += 1
            for r in records:
                if not isinstance(r, dict):
                    continue
                tid = r.get("timelogId") or r.get("TimelogId") or r.get("id")
                if tid is None:
                    continue
                upsert("timelogs", {
                    "timelog_id":           str(tid),
                    "erecno":               r.get("erecno"),
                    "employee_email":       r.get("employeeMailId"),
                    "employee_first_name":  r.get("employeeFirstName"),
                    "employee_last_name":   r.get("employeeLastName"),
                    "type":                 r.get("type"),
                    "work_date":            r.get("workDate"),
                    "hours":                r.get("hours"),
                    "total_time":           r.get("totaltime") or r.get("totalTime"),
                    "from_time":            r.get("fromTime"),
                    "to_time":              r.get("toTime"),
                    "from_time_fmt":        r.get("fromTimeInTimeFormat"),
                    "to_time_fmt":          r.get("toTimeInTimeFormat"),
                    "job_id":               str(r["jobId"]) if r.get("jobId") is not None else None,
                    "job_name":             r.get("jobName"),
                    "project_id":           str(r["projectId"]) if r.get("projectId") is not None else None,
                    "project_name":         r.get("projectName"),
                    "client_id":            str(r["clientId"]) if r.get("clientId") is not None else None,
                    "client_name":          r.get("clientName"),
                    "billing_status":       r.get("billingStatus"),
                    "approval_status":      r.get("approvalStatus"),
                    "billed_status":        r.get("billedStatus"),
                    "is_pushed_to_qbo":     r.get("isTimelogPushedToQBO"),
                    "raw_record":           json.dumps(r),
                }, id_key="timelog_id")
                n_total += 1
                if n_total % CHECKPOINT_EVERY == 0:
                    op.checkpoint(state=state)

            log.fine(f"  timelogs chunk #{chunks} page {page}: "
                     f"{len(records)} (total {n_total})")
            if len(records) < page_size:
                break
            cur += page_size

        # Move window one day before the chunk_start so we don't re-fetch.
        chunk_end = chunk_start - timedelta(days=1)

    if n_total == 0:
        log.warning(f"timelogs: 0 rows. First-call response snippet: "
                    f"{first_body_snippet}")
    log.info(f"timelogs: {n_total} log(s) across {chunks} monthly chunk(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  LMS courses (optional)
# ═══════════════════════════════════════════════════════════════════════════
def sync_courses(configuration: dict, state: dict):
    """`GET /api/v1/courses` — list every course in LMS.

    Gated on `sync_lms_courses=true` in config (because it needs a separate
    `ZOHOPEOPLE.training.READ` scope on the refresh_token)."""
    if not config_bool(configuration, "sync_lms_courses"):
        return

    host = api_host(configuration)
    url = f"{host}/api/v1/courses"

    page_size = 100
    cur = 0
    page = 0
    n_total = 0
    while True:
        params = {"sIndex": cur, "limit": page_size}
        try:
            body = api_request(configuration, url, params=params)
        except ScopeMissing as e:
            log.warning(f"courses: ZOHOPEOPLE.training.READ scope missing — "
                        f"skipping. {e!s}")
            return

        records = []
        if isinstance(body, list):
            records = body
        elif isinstance(body, dict):
            inner = body.get("response", body)
            if isinstance(inner, dict):
                records = inner.get("result") or inner.get("courses") or []
            elif isinstance(inner, list):
                records = inner

        if not isinstance(records, list):
            records = []

        page += 1
        for r in records:
            if not isinstance(r, dict):
                continue
            cid = r.get("courseId") or r.get("id")
            if cid is None:
                continue
            upsert("courses", {
                "course_id":   str(cid),
                "name":        r.get("courseName") or r.get("name"),
                "description": r.get("description"),
                "course_type": r.get("courseType") or r.get("type"),
                "status":      r.get("status"),
                "raw_record":  json.dumps(r),
            }, id_key="course_id")
            n_total += 1

        if len(records) < page_size:
            break
        cur += page_size

    log.info(f"courses: {n_total} course(s)")
    reconcile_deletes("courses", ids_seen("courses"), state, key_template="course_id")


# ═══════════════════════════════════════════════════════════════════════════
#  Timetracker — clients
# ═══════════════════════════════════════════════════════════════════════════
def sync_timetracker_clients(configuration: dict, state: dict):
    """`GET /people/api/timetracker/getclients?sIndex=&limit=200`.

    Org-wide list of clients. Required to join timelogs/projects/jobs to
    billable customers. Rate limit: 20/5min."""
    host = api_host(configuration)
    url = f"{host}/people/api/timetracker/getclients"

    log.info("timetracker_clients: starting")
    page_size = 200
    cur = 0
    n_total = 0
    first_body_snippet = None
    while True:
        params = {"sIndex": cur, "limit": page_size}
        try:
            body = api_request(configuration, url, params=params)
        except ScopeMissing as e:
            log.warning(f"timetracker_clients: scope missing — skipping. {e!s}")
            return

        if first_body_snippet is None:
            first_body_snippet = str(body)[:300]

        inner = body.get("response", body) if isinstance(body, dict) else {}
        records = inner.get("result") if isinstance(inner, dict) else []
        if not isinstance(records, list):
            records = []
        is_next = bool(inner.get("isNextAvailable") if isinstance(inner, dict) else False)

        if not records:
            break
        for r in records:
            if not isinstance(r, dict):
                continue
            cid = r.get("clientId") or r.get("ClientId") or r.get("id")
            if cid is None:
                continue
            upsert("timetracker_clients", {
                "client_id":     str(cid),
                "client_name":   r.get("clientName"),
                "currency_code": r.get("currencyCode"),
                "billing_method": r.get("billingMethod"),
                "email_id":      r.get("emailId"),
                "first_name":    r.get("firstName"),
                "last_name":     r.get("lastName"),
                "phone_no":      r.get("phoneNo"),
                "mobile_no":     r.get("mobileNo"),
                "fax_no":        r.get("faxNo"),
                "street_addr":   r.get("streetAddr"),
                "city":          r.get("city"),
                "state":         r.get("state"),
                "pincode":       r.get("pincode"),
                "country":       r.get("country"),
                "industry":      r.get("industry"),
                "comp_size":     r.get("compsize"),
                "description":   r.get("description"),
                "raw_record":    json.dumps(r),
            }, id_key="client_id")
            n_total += 1
        if not is_next or len(records) < page_size:
            break
        cur += page_size

    if n_total == 0:
        log.warning(f"timetracker_clients: 0 rows. Snippet: {first_body_snippet}")
    log.info(f"timetracker_clients: {n_total} client(s)")
    reconcile_deletes("timetracker_clients", ids_seen("timetracker_clients"),
                      state, key_template="client_id")


# ═══════════════════════════════════════════════════════════════════════════
#  Timetracker — projects
# ═══════════════════════════════════════════════════════════════════════════
def sync_timetracker_projects(configuration: dict, state: dict):
    """`GET /people/api/timetracker/getprojects?assignedTo=all&clientId=all&...`.

    Org-wide projects. Carries project cost, owner, status, billable rates,
    project managers, head, departments. Rate limit: 20/5min."""
    host = api_host(configuration)
    url = f"{host}/people/api/timetracker/getprojects"

    log.info("timetracker_projects: starting")
    page_size = 200
    cur = 0
    n_total = 0
    first_body_snippet = None
    while True:
        # Do NOT send clientId / projectManager — Zoho rejects the string
        # "all" with code 7204 ("Wrong datatype for the Parameter Input")
        # for those two; the docs claim they default to all when omitted.
        params = {
            "assignedTo":    "all",
            "projectStatus": "all",
            "isUserCount":   "true",
            "isJobCount":    "true",
            "sIndex":        cur,
            "limit":         page_size,
        }
        try:
            body = api_request(configuration, url, params=params)
        except ScopeMissing as e:
            log.warning(f"timetracker_projects: scope missing — skipping. {e!s}")
            return

        if first_body_snippet is None:
            first_body_snippet = str(body)[:300]

        inner = body.get("response", body) if isinstance(body, dict) else {}
        records = inner.get("result") if isinstance(inner, dict) else []
        if not isinstance(records, list):
            records = []
        is_next = bool(inner.get("isNextAvailable") if isinstance(inner, dict) else False)

        if not records:
            break
        for r in records:
            if not isinstance(r, dict):
                continue
            pid = r.get("projectId") or r.get("id")
            if pid is None:
                continue
            head = r.get("projectHead") or {}
            upsert("timetracker_projects", {
                "project_id":          str(pid),
                "project_name":        r.get("projectName"),
                "project_status":      r.get("projectStatus"),
                "project_cost":        r.get("projectCost"),
                "client_id":           str(r["clientId"]) if r.get("clientId") is not None else None,
                "client_name":         r.get("clientName"),
                "owner_id":            r.get("ownerId"),
                "owner_name":          r.get("ownerName"),
                "project_head_emp_id": (head.get("empId") if isinstance(head, dict) else None),
                "project_head_name":   (head.get("name") if isinstance(head, dict) else None),
                "project_head_rate":   (head.get("rate") if isinstance(head, dict) else None),
                "user_count":          r.get("projectUsersCount"),
                "job_count":           r.get("jobCount"),
                "managers":            json.dumps(r.get("projectManagers") or []),
                "users":               json.dumps(r.get("projectUsers") or []),
                "departments":         json.dumps(r.get("projectDepts") or []),
                "raw_record":          json.dumps(r),
            }, id_key="project_id")
            n_total += 1
        if not is_next or len(records) < page_size:
            break
        cur += page_size

    if n_total == 0:
        log.warning(f"timetracker_projects: 0 rows. Snippet: {first_body_snippet}")
    log.info(f"timetracker_projects: {n_total} project(s)")
    reconcile_deletes("timetracker_projects", ids_seen("timetracker_projects"),
                      state, key_template="project_id")


# ═══════════════════════════════════════════════════════════════════════════
#  Timetracker — timesheets (entity, separate from individual timelogs)
# ═══════════════════════════════════════════════════════════════════════════
def sync_timesheets(configuration: dict, state: dict):
    """`GET /people/api/timetracker/gettimesheet?user=all&fromDate=&toDate=&sIndex=&limit=200`.

    Timesheet entity: a periodic submission/approval wrapper around
    individual timelogs (timesheet = many timelogs). Has billable_hours,
    approval status, currency, totals, approved amounts. Useful for
    payroll/billing analytics.

    Rate limit: 50/min. We chunk by month to mirror timelogs' constraint
    (the docs don't state a window cap on gettimesheet but better safe)."""
    past_days = config_int(configuration, "timesheets_past_window_days", 180)
    now = datetime.now(timezone.utc).date()
    earliest = now - timedelta(days=past_days)

    host = api_host(configuration)
    url = f"{host}/people/api/timetracker/gettimesheet"

    log.info(f"timesheets: walking [{earliest} → {now}] in 1-month chunks")
    n_total = 0
    chunks = 0
    chunk_end = now
    first_body_snippet = None
    while chunk_end >= earliest:
        chunk_start = max(earliest, chunk_end - timedelta(days=30))
        chunks += 1
        page_size = 200
        cur = 0
        while True:
            params = {
                "user":           "all",
                "fromDate":       chunk_start.strftime("%Y-%m-%d"),
                "toDate":         chunk_end.strftime("%Y-%m-%d"),
                "dateFormat":     "yyyy-MM-dd",
                "approvalStatus": "all",
                "employeeStatus": "usersandnonusers",
                "sIndex":         cur,
                "limit":          page_size,
            }
            try:
                body = api_request(configuration, url, params=params)
            except ScopeMissing as e:
                log.warning(f"timesheets: scope missing — skipping. {e!s}")
                return

            if first_body_snippet is None:
                first_body_snippet = str(body)[:300]

            inner = body.get("response", body) if isinstance(body, dict) else {}
            records = inner.get("result") if isinstance(inner, dict) else []
            if not isinstance(records, list):
                records = []

            if not records:
                break
            for r in records:
                if not isinstance(r, dict):
                    continue
                tid = (r.get("recordId") or r.get("timesheetId") or r.get("id"))
                if tid is None:
                    continue
                upsert("timesheets", {
                    "timesheet_id":           str(tid),
                    "timesheet_name":         r.get("timesheetName"),
                    "employee_name":          r.get("employeeName"),
                    "employee_email":         r.get("employeeEmail"),
                    "from_date":              r.get("fromDate"),
                    "to_date":                r.get("toDate"),
                    "status":                 r.get("status"),
                    "billable_hours":         r.get("billableHours"),
                    "non_billable_hours":     r.get("nonbillableHours"),
                    "total_hours":            r.get("totalHours"),
                    "approved_billable_hours": r.get("approvedBillableHours"),
                    "approved_total_hours":   r.get("approvedTotalHours"),
                    "rate_per_hour":          r.get("ratePerHour"),
                    "total_amount":           r.get("totalAmount"),
                    "approved_total_amount":  r.get("approvedTotalAmount"),
                    "currency":               r.get("currency"),
                    "raw_record":             json.dumps(r),
                }, id_key="timesheet_id")
                n_total += 1
                if n_total % CHECKPOINT_EVERY == 0:
                    op.checkpoint(state=state)
            if len(records) < page_size:
                break
            cur += page_size
        chunk_end = chunk_start - timedelta(days=1)

    if n_total == 0:
        log.warning(f"timesheets: 0 rows. Snippet: {first_body_snippet}")
    log.info(f"timesheets: {n_total} timesheet(s) across {chunks} monthly chunk(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  Attendance — regularization (correction) requests
# ═══════════════════════════════════════════════════════════════════════════
def sync_attendance_regularization(configuration: dict, state: dict):
    """`GET /people/api/attendance/getRegularizationRecords?fromdate=&todate=&startIndex=`.

    Attendance correction/regularization requests. Critical for HR audit
    trails — every time an admin or employee edits an attendance entry,
    it creates a regularization record with before/after timestamps and
    an approval workflow.

    Rate limit: 30/5min. Window: same `attendance_past_window_days` as
    `attendance_daily` (default 180 days)."""
    past_days = config_int(configuration, "attendance_past_window_days", 180)
    now = datetime.now(timezone.utc).date()
    fromdate = (now - timedelta(days=past_days)).strftime("%Y-%m-%d")
    todate = now.strftime("%Y-%m-%d")

    host = api_host(configuration)
    url = f"{host}/people/api/attendance/getRegularizationRecords"

    log.info(f"attendance_regularization: window [{fromdate} → {todate}]")
    page_size = 200
    cur = 0
    n_total = 0
    first_body_snippet = None
    while True:
        params = {
            "fromdate":   fromdate,
            "todate":     todate,
            "dateFormat": "yyyy-MM-dd",
            "startIndex": cur,
        }
        try:
            body = api_request(configuration, url, params=params)
        except ScopeMissing as e:
            log.warning(f"attendance_regularization: scope missing — skipping. {e!s}")
            return

        if first_body_snippet is None:
            first_body_snippet = str(body)[:300]

        inner = body.get("response", body) if isinstance(body, dict) else {}
        records = inner.get("result") if isinstance(inner, dict) else []
        if not isinstance(records, list):
            records = []
        if not records:
            break
        for r in records:
            if not isinstance(r, dict):
                continue
            rid = r.get("recordId") or r.get("id")
            if rid is None:
                continue
            upsert("attendance_regularization", {
                "record_id":       str(rid),
                "approval_status": r.get("approvalStatus"),
                "employee_name":   r.get("employeeName"),
                "employee_id":     r.get("employeeId"),
                "start_date":      r.get("startDate"),
                "end_date":        r.get("endDate"),
                "reg_details":     json.dumps(r.get("regDetails") or []),
                "raw_record":      json.dumps(r),
            }, id_key="record_id")
            n_total += 1
            if n_total % CHECKPOINT_EVERY == 0:
                op.checkpoint(state=state)
        if len(records) < page_size:
            break
        cur += page_size

    if n_total == 0:
        log.warning(f"attendance_regularization: 0 rows. Snippet: {first_body_snippet}")
    log.info(f"attendance_regularization: {n_total} record(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  Attendance — per-day per-employee entries (opt-in, expensive)
# ═══════════════════════════════════════════════════════════════════════════
def sync_attendance_entries(configuration: dict, state: dict):
    """`GET /people/api/attendance/getAttendanceEntries?date=&empId=`.

    One employee + one day per call → N_employees × N_days requests. For
    a 21-employee org with default 14-day window = 294 calls. Rate limit
    is shared with the rest of the attendance endpoints, so this can
    chew through quota fast.

    OPT-IN via `sync_attendance_entries=true` in config. Window:
    `attendance_entries_past_window_days` (default 14).

    Emits:
      - `attendance_entries`         one row per (employee, date) summary
      - `attendance_entries_punches` one row per check-in/check-out pair
    """
    if not config_bool(configuration, "sync_attendance_entries"):
        log.fine("attendance_entries: disabled (sync_attendance_entries=false)")
        return

    employees = _discover_employees(configuration)
    if not employees:
        log.warning("attendance_entries: no employees discovered — skipping")
        return

    past_days = config_int(configuration, "attendance_entries_past_window_days", 14)
    now = datetime.now(timezone.utc).date()
    earliest = now - timedelta(days=past_days)

    host = api_host(configuration)
    url = f"{host}/people/api/attendance/getAttendanceEntries"

    n_entries = 0
    n_punches = 0
    n_skipped = 0
    log.info(f"attendance_entries: {len(employees)} employee(s) × "
             f"{past_days} day(s) = up to {len(employees) * past_days} call(s)")

    for emp in employees:
        emp_id = emp.get("employee_id") or emp.get("erecno") or emp.get("email_id")
        if not emp_id:
            n_skipped += 1
            continue
        emp_id_str = str(emp_id)

        cur_date = earliest
        while cur_date <= now:
            date_str = cur_date.strftime("%Y-%m-%d")
            params = {
                "empId":      emp.get("employee_id") or "",
                "emailId":    emp.get("email_id") or "",
                "erecno":     emp.get("erecno") or "",
                "date":       date_str,
                "dateFormat": "yyyy-MM-dd",
            }
            # Drop empty identifier params so Zoho picks one cleanly.
            params = {k: v for k, v in params.items() if v}
            try:
                body = api_request(configuration, url, params=params)
            except ScopeMissing as e:
                log.warning(f"attendance_entries: scope missing — stopping. {e!s}")
                return
            except Exception as e:
                log.warning(f"attendance_entries({emp_id_str}, {date_str}): {e!r}; "
                            f"continuing")
                cur_date += timedelta(days=1)
                continue

            inner = body.get("response", body) if isinstance(body, dict) else {}
            result = inner.get("result") if isinstance(inner, dict) else None
            # Result is sometimes a list, sometimes a dict.
            if isinstance(result, list) and result:
                result = result[0]
            if not isinstance(result, dict):
                cur_date += timedelta(days=1)
                continue
            if not result.get("firstIn") and not result.get("entries"):
                cur_date += timedelta(days=1)
                continue

            upsert("attendance_entries", {
                "employee_id":        emp_id_str,
                "date":               date_str,
                "employee_email":     emp.get("email_id"),
                "employee_first_name": emp.get("first_name"),
                "employee_last_name":  emp.get("last_name"),
                "first_in":           result.get("firstIn"),
                "last_out":           result.get("lastOut"),
                "total_hours":        result.get("totalHrs"),
                "status":             result.get("status"),
                "paid_break":         result.get("paidBreak"),
                "unpaid_break":       result.get("unPaidBreak"),
                "allowed_to_checkin": result.get("allowedToCheckIn"),
                "raw_record":         json.dumps(result),
            }, id_key="employee_id|date")
            n_entries += 1

            entries = result.get("entries") or []
            if isinstance(entries, list):
                for idx, punch in enumerate(entries):
                    if not isinstance(punch, dict):
                        continue
                    upsert("attendance_entries_punches", {
                        "employee_id":         emp_id_str,
                        "date":                date_str,
                        "punch_idx":           idx,
                        "check_in":            punch.get("checkIn"),
                        "check_out":           punch.get("checkOut"),
                        "check_in_location":   punch.get("checkIn_Location"),
                        "check_out_location":  punch.get("checkOut_Location"),
                        "source_of_punch_in":  punch.get("sourceOfPunchIn"),
                        "source_of_punch_out": punch.get("sourceOfPunchOut"),
                        "raw_punch":           json.dumps(punch),
                    }, id_key="employee_id|date|punch_idx")
                    n_punches += 1

            if n_entries % CHECKPOINT_EVERY == 0:
                op.checkpoint(state=state)
            cur_date += timedelta(days=1)

    log.info(f"attendance_entries: {n_entries} day-row(s), {n_punches} punch-pair(s) "
             f"({n_skipped} employee(s) skipped — no identifier)")


# ═══════════════════════════════════════════════════════════════════════════
#  Attendance — latest entries (recent snapshot, opt-in)
# ═══════════════════════════════════════════════════════════════════════════
def sync_attendance_latest_entries(configuration: dict, state: dict):
    """`GET /api/attendance/fetchLatestAttEntries?duration=<minutes>&dateTimeFormat=`.

    Returns attendance entries org-wide added/modified in the last
    `duration_minutes`. One API call. Use case: near-real-time
    dashboards. Opt-in via `sync_attendance_latest_entries=true`.
    """
    if not config_bool(configuration, "sync_attendance_latest_entries"):
        log.fine("attendance_latest_entries: disabled")
        return

    duration_minutes = config_int(configuration,
                                  "attendance_latest_entries_duration_minutes", 1440)

    host = api_host(configuration)
    url = f"{host}/api/attendance/fetchLatestAttEntries"
    params = {
        "duration":       duration_minutes,
        "dateTimeFormat": "yyyy-MM-dd HH:mm:ss",
    }
    try:
        body = api_request(configuration, url, params=params)
    except ScopeMissing as e:
        log.warning(f"attendance_latest_entries: scope missing — skipping. {e!s}")
        return

    inner = body.get("response", body) if isinstance(body, dict) else {}
    employees = inner.get("result") if isinstance(inner, dict) else []
    if not isinstance(employees, list):
        employees = []

    n_total = 0
    for emp in employees:
        if not isinstance(emp, dict):
            continue
        emp_id = emp.get("employeeId") or emp.get("erecNo")
        if not emp_id:
            continue
        emp_entries = emp.get("entries") or {}
        if not isinstance(emp_entries, dict):
            continue
        for date_str, day in emp_entries.items():
            if not isinstance(day, dict):
                continue
            # Three lists per day: singleRegEntries, multiRegEntries, attEntries.
            for source_key in ("attEntries", "singleRegEntries", "multiRegEntries"):
                arr = day.get(source_key) or []
                if not isinstance(arr, list):
                    continue
                for idx, entry in enumerate(arr):
                    if not isinstance(entry, dict):
                        continue
                    upsert("attendance_latest_entries", {
                        "employee_id":          str(emp_id),
                        "date":                 date_str,
                        "entry_idx":            f"{source_key}_{idx}",
                        "source":               source_key,
                        "employee_first_name":  emp.get("firstName"),
                        "employee_last_name":   emp.get("lastName"),
                        "employee_email":       emp.get("emailId"),
                        "erecno":               emp.get("erecNo"),
                        "check_in":             entry.get("checkIn"),
                        "check_out":            entry.get("checkOut"),
                        "check_in_location":    entry.get("checkInLocation"),
                        "check_out_location":   entry.get("checkOutLocation"),
                        "break_id":             entry.get("breakId"),
                        "break_name":           entry.get("breakName"),
                        "raw_entry":            json.dumps(entry),
                    }, id_key="employee_id|date|entry_idx")
                    n_total += 1

    log.info(f"attendance_latest_entries: {n_total} entry(s) "
             f"(window={duration_minutes}min)")


# ═══════════════════════════════════════════════════════════════════════════
#  Attendance — shift mappings (per-employee, cheap)
# ═══════════════════════════════════════════════════════════════════════════
_DDMMMYYYY_MONTHS = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


def _normalize_zoho_date(s):
    """Convert Zoho's `dd-MMM-yyyy` (e.g. `29-Nov-2025`) to ISO `yyyy-MM-dd`.
    Pass through values that are already ISO or unparseable."""
    if not isinstance(s, str) or len(s) != 11 or s[2] != "-" or s[6] != "-":
        return s
    day, mon, year = s[:2], s[3:6], s[7:]
    mm = _DDMMMYYYY_MONTHS.get(mon)
    if not mm or not day.isdigit() or not year.isdigit():
        return s
    return f"{year}-{mm}-{day}"


def sync_shift_mappings(configuration: dict, state: dict):
    """`GET /people/api/attendance/getShiftConfiguration?empId=&sdate=&edate=`.

    Per-employee per-day shift assignments. Response shape (observed,
    not the documented `{response:{result}}`):

        {"userShiftDetails": {
            "employeeName": "...",
            "erecno":       "...",
            "employeeId":   "...",
            "emailId":      "...",
            "locationId":   "...",
            "isShiftEditable": True,
            "shiftList": [
              {"date": "29-Nov-2025", "shiftName": "...", "startTime": "09:00",
               "endTime": "18:00", "weekend": {...}, "holiday": {...}}
            ]
        }}

    **The endpoint caps each query at a 1-month window** (`{"error":
    "Date period should be within 1 month"}`). We walk in 28-day chunks.

    Rate-limit note: the per-endpoint limiter (25/5min) means a long
    backfill across many employees pauses 5 min between every 25 chunks.
    For 21 employees × ~2 chunks (60-day default) = 42 calls →
    one ~5-minute pause. Tune `shift_mappings_past_window_days` to
    trade range for runtime."""
    employees = _discover_employees(configuration)
    if not employees:
        log.warning("shift_mappings: no employees discovered — skipping")
        return

    # Defaults to 60 days to keep the rate-limit cost reasonable; can be
    # increased via config if longer history is needed.
    past_days = config_int(configuration, "shift_mappings_past_window_days", 60)
    now = datetime.now(timezone.utc).date()
    earliest = now - timedelta(days=past_days)

    host = api_host(configuration)
    url = f"{host}/people/api/attendance/getShiftConfiguration"

    n_total = 0
    n_emp_with_data = 0
    n_chunks = 0
    first_body_snippet = None

    for emp in employees:
        emp_id = emp.get("employee_id") or emp.get("erecno") or emp.get("email_id")
        if not emp_id:
            continue
        emp_id_str = str(emp_id)
        emp_had_data = False

        chunk_start = earliest
        while chunk_start <= now:
            chunk_end = min(now, chunk_start + timedelta(days=28))
            n_chunks += 1
            params = {
                "empId":      emp.get("employee_id") or "",
                "emailId":    emp.get("email_id") or "",
                "erecno":     emp.get("erecno") or "",
                "sdate":      chunk_start.strftime("%Y-%m-%d"),
                "edate":      chunk_end.strftime("%Y-%m-%d"),
                "dateFormat": "yyyy-MM-dd",
            }
            params = {k: v for k, v in params.items() if v}
            try:
                body = api_request(configuration, url, params=params)
            except ScopeMissing as e:
                log.warning(f"shift_mappings: scope missing — stopping. {e!s}")
                return
            except Exception as e:
                log.fine(f"shift_mappings({emp_id_str}, "
                         f"{chunk_start}→{chunk_end}): {e!r}; continuing")
                chunk_start = chunk_end + timedelta(days=1)
                continue

            if first_body_snippet is None:
                first_body_snippet = str(body)[:300]

            # Real shape: { userShiftDetails: { shiftList: [...] , <emp meta>}}
            shift_list = []
            usd = {}
            if isinstance(body, dict):
                usd = body.get("userShiftDetails")
                if isinstance(usd, dict):
                    cand = usd.get("shiftList")
                    if isinstance(cand, list):
                        shift_list = cand
                # Fallback paths (docs-claimed envelope).
                if not shift_list:
                    inner = body.get("response", body)
                    if isinstance(inner, dict):
                        cand = (inner.get("result") or inner.get("shifts")
                                or inner.get("shiftDetails"))
                        if isinstance(cand, list):
                            shift_list = cand

            for s in shift_list:
                if not isinstance(s, dict):
                    continue
                date_str = (s.get("date") or s.get("Date")
                            or s.get("shiftDate"))
                if not date_str:
                    continue
                date_iso = _normalize_zoho_date(date_str)
                shift_name = (s.get("shiftName") or s.get("ShiftName")
                              or s.get("name"))
                upsert("shift_mappings", {
                    "employee_id":      emp_id_str,
                    "date":             date_iso,
                    "shift_name":       shift_name,
                    "shift_start_time": s.get("startTime") or s.get("StartTime"),
                    "shift_end_time":   s.get("endTime") or s.get("EndTime"),
                    "weekend":          json.dumps(s.get("weekend")
                                                   or s.get("Weekend") or {}),
                    "holiday":          json.dumps(s.get("holiday")
                                                   or s.get("Holiday") or {}),
                    "employee_name":    (usd.get("employeeName")
                                         if isinstance(usd, dict) else None),
                    "employee_email":   (usd.get("emailId")
                                         if isinstance(usd, dict) else None) or emp.get("email_id"),
                    "erecno":           (usd.get("erecno")
                                         if isinstance(usd, dict) else None),
                    "location_id":      (usd.get("locationId")
                                         if isinstance(usd, dict) else None),
                    "is_shift_editable": (usd.get("isShiftEditable")
                                          if isinstance(usd, dict) else None),
                    "raw_record":       json.dumps(s),
                }, id_key="employee_id|date")
                n_total += 1
                emp_had_data = True

            chunk_start = chunk_end + timedelta(days=1)

        if emp_had_data:
            n_emp_with_data += 1

    if n_total == 0:
        log.warning(f"shift_mappings: 0 rows. Snippet: {first_body_snippet}")
    log.info(f"shift_mappings: {n_total} day-mapping(s) across "
             f"{n_emp_with_data} employee(s), {n_chunks} chunk(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  Cases (HR helpdesk)
# ═══════════════════════════════════════════════════════════════════════════
def sync_cases(configuration: dict, state: dict):
    """`GET /api/hrcases/getAllCases?index=1&status=1,2,3,4,5&periodOfTime=`.

    HR helpdesk tickets. Statuses: 1=Open, 2=In Progress, 3=Awaiting
    Requestor, 4=On Hold, 5=Closed. 25 records per page. Rate limit:
    30/5min."""
    host = api_host(configuration)
    url = f"{host}/api/hrcases/getAllCases"

    log.info("cases: starting")
    page_size = 25  # docs cap
    cur = 1        # 1-indexed
    n_total = 0
    first_body_snippet = None
    while True:
        params = {
            "index":  cur,
            "status": "1,2,3,4,5",  # all statuses
        }
        try:
            body = api_request(configuration, url, params=params)
        except ScopeMissing as e:
            log.warning(f"cases: scope missing — skipping. {e!s}")
            return

        if first_body_snippet is None:
            first_body_snippet = str(body)[:300]

        # Shape: {hrcaseList: [...], isNextAvailable: bool}
        if not isinstance(body, dict):
            break
        records = body.get("hrcaseList") or body.get("result") or []
        if not isinstance(records, list):
            records = []
        is_next = bool(body.get("isNextAvailable"))

        if not records:
            break
        for r in records:
            if not isinstance(r, dict):
                continue
            cid = r.get("caseId") or r.get("recordId") or r.get("id")
            if cid is None:
                continue
            requestor = r.get("requestor") or {}
            agent = r.get("agent") or {}
            sla = r.get("SLA") or r.get("sla") or {}
            upsert("cases", {
                "case_id":        str(cid),
                "record_id":      str(r.get("recordId") or cid),
                "subject":        r.get("subject"),
                "status":         r.get("status"),
                "status_id":      r.get("statusId"),
                "category_name":  r.get("categoryName"),
                "category_id":    r.get("categoryId"),
                "agent_name":     (agent.get("name") if isinstance(agent, dict) else None),
                "agent_id":       (agent.get("erecno") or agent.get("empId")
                                   if isinstance(agent, dict) else None),
                "requestor_name": (requestor.get("name") if isinstance(requestor, dict) else None),
                "requestor_id":   (requestor.get("erecno") or requestor.get("empId")
                                   if isinstance(requestor, dict) else None),
                "sla":            json.dumps(sla),
                "has_attachment": r.get("hasAttachment"),
                "created_time":   r.get("createdTime"),
                "raw_record":     json.dumps(r),
            }, id_key="case_id")
            n_total += 1
        if not is_next:
            break
        cur += 1  # 1-indexed page counter

    if n_total == 0:
        log.warning(f"cases: 0 rows. Snippet: {first_body_snippet}")
    log.info(f"cases: {n_total} case(s)")
    reconcile_deletes("cases", ids_seen("cases"), state, key_template="case_id")


# ═══════════════════════════════════════════════════════════════════════════
#  Announcements
# ═══════════════════════════════════════════════════════════════════════════
def sync_announcements(configuration: dict, state: dict):
    """`GET /people/api/announcement/getAllAnnouncement?startIdx=&isSticky=`.

    Org-wide announcement list. 10 records per page (tight). Rate limit:
    10/5min — very slow, so we cap at 50 pages = 500 announcements per
    run, which is plenty for any normal org."""
    host = api_host(configuration)
    url = f"{host}/people/api/announcement/getAllAnnouncement"

    log.info("announcements: starting")
    page_size = 10  # docs hard cap
    cur = 1         # 1-indexed
    max_pages = 50
    n_total = 0
    pages = 0
    first_body_snippet = None
    while pages < max_pages:
        params = {"startIdx": cur, "isSticky": "false"}
        try:
            body = api_request(configuration, url, params=params)
        except ScopeMissing as e:
            log.warning(f"announcements: scope missing — skipping. {e!s}")
            return

        if first_body_snippet is None:
            first_body_snippet = str(body)[:300]

        # Shape: {result: {resultData: {announcementList: [...], hasNext: bool}}}
        records = []
        has_next = False
        if isinstance(body, dict):
            res = body.get("result")
            if isinstance(res, dict):
                rd = res.get("resultData", res)
                if isinstance(rd, dict):
                    records = rd.get("announcementList") or []
                    has_next = bool(rd.get("hasNext"))
        if not isinstance(records, list):
            records = []

        pages += 1
        if not records:
            break
        for r in records:
            if not isinstance(r, dict):
                continue
            aid = r.get("announcementId") or r.get("id")
            if aid is None:
                continue
            upsert("announcements", {
                "announcement_id":      str(aid),
                "subject":              r.get("subject"),
                "message":              r.get("message"),
                "owner":                r.get("owner"),
                "publish_date":         r.get("publishDate"),
                "expire_date":          r.get("expireDate"),
                "is_active":            r.get("isActive"),
                "is_sticky":            r.get("isSticky"),
                "is_comment_disable":   r.get("isCommentDisable"),
                "is_notify":            r.get("isNotify"),
                "modified_time":        r.get("modifiedTime"),
                "notify_others_emails": r.get("notifyOthersEmailIds"),
                "raw_record":           json.dumps(r),
            }, id_key="announcement_id")
            n_total += 1
        if not has_next:
            break
        cur += page_size

    if n_total == 0:
        log.warning(f"announcements: 0 rows. Snippet: {first_body_snippet}")
    log.info(f"announcements: {n_total} announcement(s) across {pages} page(s)")
    reconcile_deletes("announcements", ids_seen("announcements"), state,
                      key_template="announcement_id")


# ═══════════════════════════════════════════════════════════════════════════
#  LMS — learner course progress (per-employee, opt-in via sync_lms_courses)
# ═══════════════════════════════════════════════════════════════════════════
def sync_learner_progress(configuration: dict, state: dict):
    """`GET /api/v1/learners/<learnerId>/course-progress?startIndex=`.

    Per-learner course progress. Gated on `sync_lms_courses=true` (same
    flag as `sync_courses`) because both need the `ZOHOPEOPLE.training.READ`
    scope. Iterates over discovered employees.

    Emits one row per (learner_id, course_id) with completion %, marks,
    enrollment/completion dates."""
    if not config_bool(configuration, "sync_lms_courses"):
        return

    employees = _discover_employees(configuration)
    if not employees:
        log.warning("learner_progress: no employees discovered — skipping")
        return

    host = api_host(configuration)
    n_total = 0
    n_learners = 0
    first_body_snippet = None
    for emp in employees:
        # LMS uses erecno as the learnerId most reliably.
        learner_id = emp.get("erecno") or emp.get("employee_id") or emp.get("email_id")
        if not learner_id:
            continue
        learner_id_str = str(learner_id)
        cur = 0
        page_size = 200
        while True:
            url = f"{host}/api/v1/learners/{learner_id_str}/course-progress"
            params = {"startIndex": cur}
            try:
                body = api_request(configuration, url, params=params)
            except ScopeMissing as e:
                log.warning(f"learner_progress: scope missing — stopping. {e!s}")
                return
            except Exception as e:
                log.fine(f"learner_progress({learner_id_str}): {e!r}; continuing")
                break

            if first_body_snippet is None:
                first_body_snippet = str(body)[:300]

            progress_list = []
            if isinstance(body, dict):
                progress_list = body.get("courseProgress") or []
            if not isinstance(progress_list, list):
                progress_list = []
            if not progress_list:
                break

            n_learners_this = 0
            for p in progress_list:
                if not isinstance(p, dict):
                    continue
                cid = p.get("courseId")
                if not cid:
                    continue
                upsert("learner_progress", {
                    "learner_id":              learner_id_str,
                    "course_id":               str(cid),
                    "course_name":             p.get("courseName"),
                    "course_type":             p.get("courseType"),
                    "batch_id":                p.get("batchId"),
                    "batch_name":              p.get("batchName"),
                    "enrolled_date":           p.get("enrolledDate"),
                    "completion_date":         p.get("completionDate"),
                    "completion_percentage":   p.get("completionPercentage"),
                    "completion_status":       p.get("completionStatus"),
                    "completed_entities":      p.get("completedEntitiesCount"),
                    "total_entities":          p.get("totalEntitiesCount"),
                    "manager_evaluation_marks": p.get("managerEvaluationMarks"),
                    "raw_record":              json.dumps(p),
                }, id_key="learner_id|course_id")
                n_total += 1
                n_learners_this += 1
            if n_learners_this:
                n_learners += 1
            if len(progress_list) < page_size:
                break
            cur += page_size

    if n_total == 0:
        log.warning(f"learner_progress: 0 rows. Snippet: {first_body_snippet}")
    log.info(f"learner_progress: {n_total} (learner, course) row(s) "
             f"across {n_learners} learner(s)")
