"""
Meta-table sync for the Zoho People connector.

Covers the small, slow-moving reference tables. All are full-sync each run
— they're tiny and downstream queries assume the snapshot is current.

Tables produced:
  - forms_meta              one row per form
  - views_meta              one row per (form, view)
  - form_fields             one row per (form, field), best-effort
  - file_categories         file/document categories
  - files                   file index (HR + company)
  - holidays                org-wide holiday calendar
  - leave_types             leave types (org-wide, derived from holidays + leave records)
"""

import json
from datetime import datetime, timedelta, timezone

from fivetran_connector_sdk import Logging as log

from api_client import api_request, ScopeMissing, unwrap_envelope
from auth import api_host
from helpers import (
    config_int,
    flatten_record_auto,
    ids_seen,
    reconcile_deletes,
    upsert,
)
from schema import discover


# ── Forms meta + views ──────────────────────────────────────────────────────
def sync_forms_meta(configuration: dict, state: dict):
    """Iterate the discovery catalog and emit forms_meta + views_meta rows.

    Discovery happens once per run (cached by `schema.discover`), so this
    is essentially zero-cost — we just re-shape the catalog into rows."""
    catalog = discover(configuration)

    n_forms = n_views = 0
    for form in catalog.get("forms", []):
        upsert("forms_meta", {
            "form_link_name": form["form_link_name"],
            "display_name":   form.get("display_name"),
            "is_custom":      form.get("is_custom"),
            "component_id":   form.get("component_id"),
            "default_view":   form.get("default_view"),
            "table_name":     form.get("table"),
            "raw_meta":       json.dumps(form.get("raw_meta") or {}),
        }, id_key="form_link_name")
        n_forms += 1

        for v in form.get("views") or []:
            upsert("views_meta", {
                "form_link_name": form["form_link_name"],
                "view_name":      v["view_name"],
                "view_id":        v.get("view_id"),
                "display_name":   v.get("display_name"),
                "is_default":     v.get("is_default"),
                "view_type":      v.get("view_type"),
                "raw_meta":       json.dumps(v.get("raw_meta") or {}),
            }, id_key="form_link_name|view_name")
            n_views += 1

    log.info(f"forms_meta: {n_forms} forms, {n_views} views")
    reconcile_deletes("forms_meta", ids_seen("forms_meta"), state,
                      key_template="form_link_name")
    reconcile_deletes("views_meta", ids_seen("views_meta"), state,
                      key_template={"form_link_name": 0, "view_name": 1})


def sync_form_fields(configuration: dict, state: dict):
    """Best-effort field discovery. The Zoho People Forms API documents a
    "Get Fields of Form" endpoint, but the exact URL varies by version
    (`/api/forms/{form}/components` on newer surfaces, undocumented in v1).
    We try the most-common path and silently skip forms where Zoho returns
    an error. Field discovery is purely diagnostic — actual record columns
    are inferred at upsert time.
    """
    catalog = discover(configuration)
    host = api_host(configuration)

    n_fields = 0
    n_skipped = 0
    for form in catalog.get("forms", []):
        form_link = form["form_link_name"]
        # Try the form-components endpoint. Falls through silently on any error.
        try:
            body = api_request(configuration,
                               f"{host}/api/forms/{form_link}/components")
        except ScopeMissing:
            n_skipped += 1
            continue
        except Exception as e:
            log.fine(f"  components fetch failed for {form_link}: {e!r}")
            n_skipped += 1
            continue

        raw_fields = []
        if isinstance(body, list):
            raw_fields = body
        elif isinstance(body, dict):
            inner = body.get("response", body)
            if isinstance(inner, dict):
                raw_fields = inner.get("result") or inner.get("fields") or []
            elif isinstance(inner, list):
                raw_fields = inner

        for fd in raw_fields or []:
            if not isinstance(fd, dict):
                continue
            label = (fd.get("labelName") or fd.get("label_name")
                     or fd.get("fieldLabel") or fd.get("displayName"))
            if not label:
                continue
            upsert("form_fields", {
                "form_link_name": form_link,
                "field_label":    label,
                "field_type":     fd.get("fieldType") or fd.get("type"),
                "display_name":   fd.get("displayName") or fd.get("display_name"),
                "is_mandatory":   fd.get("isMandatory") or fd.get("is_mandatory"),
                "is_lookup":      fd.get("isLookup") or fd.get("is_lookup"),
                "raw_meta":       json.dumps(fd),
            }, id_key="form_link_name|field_label")
            n_fields += 1

    log.info(f"form_fields: {n_fields} field(s) across forms "
             f"({n_skipped} form(s) skipped — components endpoint unavailable)")
    reconcile_deletes("form_fields", ids_seen("form_fields"), state,
                      key_template={"form_link_name": 0, "field_label": 1})


# ── File categories + files ─────────────────────────────────────────────────
def sync_file_categories(configuration: dict, state: dict):
    """`GET /people/api/files/getCategories`. Envelope: `{response: {result: [...]}}`.
    Each result row carries an inner `cats` array — the actual category
    list — plus file-level metadata. We pick out the category entries."""
    host = api_host(configuration)
    url = f"{host}/people/api/files/getCategories"
    try:
        body = api_request(configuration, url)
    except ScopeMissing as e:
        log.warning(f"file_categories: skipping — {e!s}")
        return
    except Exception as e:
        log.warning(f"file_categories: skipping — {e!r}")
        return

    try:
        inner = unwrap_envelope(body, url)
    except ScopeMissing as e:
        log.warning(f"file_categories: skipping — {e!s}")
        return
    except Exception as e:
        log.warning(f"file_categories: envelope rejected — {e!r}")
        return

    results = inner.get("result") if isinstance(inner, dict) else []
    if not isinstance(results, list):
        results = []

    n = 0
    seen_ids = set()
    for row in results:
        if not isinstance(row, dict):
            continue
        # The legacy endpoint nests categories under `cats`. Some newer
        # responses flatten them — handle both.
        cats = row.get("cats")
        if isinstance(cats, list):
            for c in cats:
                if not isinstance(c, dict):
                    continue
                cid = (c.get("getFileCatId") or c.get("fileCategoryID")
                       or c.get("categoryId") or c.get("id"))
                if cid is None or cid in seen_ids:
                    continue
                seen_ids.add(cid)
                upsert("file_categories", {
                    "file_category_id": str(cid),
                    "category_name":    (c.get("getFileCatName")
                                         or c.get("fileCatName")
                                         or c.get("name")),
                    "raw_meta":         json.dumps(c),
                }, id_key="file_category_id")
                n += 1
        else:
            cid = (row.get("fileCategoryID") or row.get("getFileCatId")
                   or row.get("categoryId") or row.get("id"))
            if cid is None or cid in seen_ids:
                continue
            seen_ids.add(cid)
            upsert("file_categories", {
                "file_category_id": str(cid),
                "category_name":    (row.get("fileCatName")
                                     or row.get("getFileCatName")
                                     or row.get("name")),
                "raw_meta":         json.dumps(row),
            }, id_key="file_category_id")
            n += 1

    log.info(f"file_categories: {n} category(s)")
    reconcile_deletes("file_categories", ids_seen("file_categories"), state,
                      key_template="file_category_id")


def sync_files(configuration: dict, state: dict):
    """`GET /people/api/files/getAllFiles?fileType={0|1}&filterBy=all&start=&limit=`.

    `fileType=1` is Company Files, `fileType=0` is HR Files. We sync both."""
    host = api_host(configuration)
    url = f"{host}/people/api/files/getAllFiles"

    def _extract(body):
        if not isinstance(body, dict):
            return None, True
        inner = body.get("response", body)
        if not isinstance(inner, dict):
            return None, True
        results = inner.get("result")
        if results in (None, "", "no records"):
            return [], True
        if isinstance(results, dict):
            # Some shapes wrap result in another dict by ID; flatten.
            results = list(results.values())
        if not isinstance(results, list):
            return [], True
        return results, False

    n_total = 0
    for file_type in (0, 1):
        page_size = 100
        cur = 0
        while True:
            extras = {"fileType": file_type, "filterBy": "all"}
            params = {"start": cur, "limit": page_size, **extras}
            try:
                body = api_request(configuration, url, params=params)
            except ScopeMissing as e:
                log.warning(f"files (fileType={file_type}): scope missing, skipping — {e!s}")
                break
            except Exception as e:
                log.warning(f"files (fileType={file_type}): page failed, "
                            f"skipping this fileType — {e!r}")
                break
            records, is_last = _extract(body)
            if records is None:
                break
            n_page = 0
            for row in records:
                if not isinstance(row, dict):
                    continue
                fid = (row.get("fileID") or row.get("fileId") or row.get("id"))
                if fid is None:
                    continue
                upsert("files", {
                    "file_id":           str(fid),
                    "file_name":         row.get("fileName"),
                    "file_path":         row.get("filePath"),
                    "file_date":         row.get("fileDate"),
                    "file_description":  row.get("fileDesc"),
                    "file_category_id":  (str(row["fileCategoryID"])
                                          if row.get("fileCategoryID") is not None
                                          else None),
                    "file_category_name": row.get("fileCatName"),
                    "file_type":         file_type,
                    "file_usage_space":  row.get("fileUsageSpace"),
                    "file_owner_name":   row.get("fileOwnerName"),
                    "raw_meta":          json.dumps(row),
                }, id_key="file_id")
                n_page += 1
                n_total += 1
            if n_page < page_size or is_last:
                break
            cur += page_size

    log.info(f"files: {n_total} file(s) across HR + Company")
    reconcile_deletes("files", ids_seen("files"), state, key_template="file_id")


# ── Holidays ────────────────────────────────────────────────────────────────
_HOLIDAY_ERROR_SHAPE_KEYS = {"error_msg", "errormessage", "error_code",
                             "errorCode", "errors"}


def _holidays_extract_rows(body) -> tuple:
    """Returns (rows, error_detail).

    Observed response shapes from `/people/api/leave/v2/holidays/get`:

      success (org has holidays in range):
        {"data": [<holiday>, ...], "message": "Data fetched successfully!",
         "uri": "...", "status": 1}

      success (org has none):
        {"data": [], "message": "Data fetched successfully!",
         "uri": "...", "status": 1}

      window too wide (1-year cap):
        {"error_msg": "Date period should be within 1 year",
         "error_code": 7014, "uri": "...", "status": 0}

    Docs claim `{"response": {"result": [...]}}` — we also accept that
    in case Zoho normalises it later."""
    if isinstance(body, list):
        return (body, None)
    if not isinstance(body, dict):
        return ([], None)
    # Error envelope first — has error_msg/error_code at top level.
    if any(k in body for k in _HOLIDAY_ERROR_SHAPE_KEYS) and \
            "data" not in body and "result" not in body and "response" not in body:
        return ([], body)
    # Real shape: top-level `data` list.
    if isinstance(body.get("data"), list):
        return (body["data"], None)
    # Docs-claimed shape: `{response: {result: [...]}}`.
    inner = body.get("response", body)
    if isinstance(inner, dict):
        if any(k in inner for k in _HOLIDAY_ERROR_SHAPE_KEYS) and \
                not inner.get("result") and not inner.get("data"):
            return ([], inner)
        rows = inner.get("result") or inner.get("data") or inner.get("holidays") or []
        if isinstance(rows, list):
            return (rows, None)
    if isinstance(inner, list):
        return (inner, None)
    return ([], None)


def sync_holidays(configuration: dict, state: dict):
    """`GET /people/api/leave/v2/holidays/get?from=...&to=...&dateFormat=yyyy-MM-dd`.

    Org-wide endpoint — returns holidays across all locations + shifts.
    Capped at a **1-year window per call** (server returns
    `{error_code: 7014}` for wider ranges). Walk the configured
    [-past, +future] range in ≤364-day chunks."""
    past_days = config_int(configuration, "holidays_past_window_days", 365)
    future_days = config_int(configuration, "holidays_future_window_days", 730)
    now = datetime.now(timezone.utc).date()
    earliest = now - timedelta(days=past_days)
    latest = now + timedelta(days=future_days)

    host = api_host(configuration)
    url = f"{host}/people/api/leave/v2/holidays/get"

    log.info(f"holidays: walking [{earliest} → {latest}] in ≤1-year chunks")
    n_total = 0
    chunks = 0
    first_body_snippet = None
    chunk_start = earliest
    while chunk_start <= latest:
        chunk_end = min(latest, chunk_start + timedelta(days=364))
        chunks += 1
        from_date = chunk_start.strftime("%Y-%m-%d")
        to_date = chunk_end.strftime("%Y-%m-%d")
        params = {"from": from_date, "to": to_date,
                  "dateFormat": "yyyy-MM-dd"}
        try:
            body = api_request(configuration, url, params=params)
        except ScopeMissing as e:
            log.warning(f"holidays: scope missing, stopping — {e!s}")
            return

        if first_body_snippet is None:
            first_body_snippet = str(body)[:500]

        rows, err = _holidays_extract_rows(body)
        if err is not None:
            log.warning(f"holidays chunk #{chunks} [{from_date}→{to_date}] "
                        f"returned error envelope, skipping: {err}")
            chunk_start = chunk_end + timedelta(days=1)
            continue

        for row in rows:
            if not isinstance(row, dict):
                continue
            hid = row.get("Id") or row.get("id")
            if hid is None:
                continue
            upsert("holidays", {
                "id":            str(hid),
                "name":          row.get("Name"),
                "date":          row.get("Date") or row.get("HolidayDate"),
                "location_id":   row.get("LocationId"),
                "location_name": row.get("LocationName"),
                "shift_id":      row.get("ShiftId"),
                "shift_name":    row.get("ShiftName"),
                "is_half_day":   row.get("isHalfday") or row.get("IsHalfday"),
                "is_restricted": (row.get("isRestrictedHoliday")
                                  or row.get("IsRestrictedHoliday")),
                "session":       row.get("Session"),
                "remarks":       row.get("Remarks"),
                "raw_meta":      json.dumps(row),
            }, id_key="id")
            n_total += 1

        log.fine(f"  holidays chunk #{chunks} [{from_date}→{to_date}]: "
                 f"{len(rows)} row(s) (overall {n_total})")
        chunk_start = chunk_end + timedelta(days=1)

    if n_total == 0:
        log.warning(f"holidays: 0 rows. First-chunk response snippet: "
                    f"{first_body_snippet}")
    log.info(f"holidays: {n_total} row(s) across {chunks} year-chunk(s)")
    reconcile_deletes("holidays", ids_seen("holidays"), state)
