"""
Data-table sync for the Zoho Creator connector.

For each REPORT discovered, sync its records. (Reports — not forms — are the
data-readable surface in v2.1. The /meta/.../reports endpoint does not include
`base_form_link_name`, so we can't map reports back to forms automatically;
each report becomes its own data table.)

Strategy per report:
  - First run / weekly full re-sync → Bulk Read full sync (catches hard-deletes)
  - Otherwise                       → Data API filtered by Modified_Time > since
  - Bulk Read failure               → fall back to Data API for that report

Subform extraction is RUNTIME-driven: any field whose value is a list of dicts
becomes a child table named `{report_table}__sub_{field_name}` with a
`parent_id` FK column. No pre-flight form-metadata required.
"""

import json

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import (
    BulkReadFailed,
    DailyLimitExceeded,
    ScopeMissing,
    bulk_read,
    fetch_records,
)


# Bulk-Read circuit breaker.
#
# Zoho's bulk API requires TWO scopes:
#   - ZohoCreator.bulk.CREATE  — to POST a new job
#   - ZohoCreator.bulk.READ    — to GET the job status + download the result
#
# Many OAuth grants in the wild have only the CREATE scope (Zoho's own docs
# omit READ from the OAuth-overview page — it's only mentioned on the
# bulk-API sub-pages). When that happens, every report's bulk attempt creates
# a job (wastes 1 API call) and then fails on the very first status poll.
#
# Two failure modes, both flip the breaker:
#   - `_bulk_disabled`        — bulk.CREATE itself is missing (POST fails)
#   - poll/download failures  — bulk.CREATE works but bulk.READ doesn't;
#     after `BULK_POLL_FAILURE_THRESHOLD` consecutive reports fail this way,
#     we stop attempting bulk for the rest of the run and just use the Data
#     API. Saves both the wasted POST and the wasted GET-poll per report.
_bulk_disabled: list = [False]
_bulk_poll_failures: list = [0]
BULK_POLL_FAILURE_THRESHOLD = 3


def reset_bulk_state():
    _bulk_disabled[0] = False
    _bulk_poll_failures[0] = 0


def _note_bulk_poll_failure():
    _bulk_poll_failures[0] += 1
    if _bulk_poll_failures[0] >= BULK_POLL_FAILURE_THRESHOLD and not _bulk_disabled[0]:
        log.warning(
            f"Bulk Read disabled for the rest of this run after "
            f"{_bulk_poll_failures[0]} consecutive poll/download failures. "
            f"The OAuth grant is missing `ZohoCreator.bulk.READ` (separate "
            f"from `ZohoCreator.bulk.CREATE`). Add it to the Self-Client "
            f"scope set and regenerate the refresh_token to enable bulk."
        )
        _bulk_disabled[0] = True


def _note_bulk_poll_success():
    _bulk_poll_failures[0] = 0
from helpers import (
    flatten_record_auto,
    mark_full_sync,
    reconcile_deletes,
    safe_table_suffix,
    should_full_sync,
    upsert,
)


def _maybe_parse_csv_cell(v):
    if v is None or v == "":
        return None
    if not isinstance(v, str):
        return v
    s = v.strip()
    if not s:
        return None
    if s[:1] in ("{", "[") and s[-1:] in ("}", "]"):
        try:
            return json.loads(s)
        except ValueError:
            return v
    return v


def _normalize_csv_row(row: dict) -> dict:
    return {k: _maybe_parse_csv_cell(v) for k, v in row.items()}


def _process_record(raw: dict, table: str, current_ids: set):
    """Auto-detect subform fields (any list-of-dicts) → child rows.
    Flatten parent → upsert. Track ID.

    Subform field names like `Contact.Name` are sanitised via
    `safe_table_suffix` so the resulting child table is
    `<parent>__sub_contact_name` (BigQuery-safe), not
    `<parent>__sub_contact.name`.
    """
    children = []
    for k, v in list(raw.items()):
        if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
            children.append((k, v))

    parent_id = raw.get("ID")
    for field_name, arr in children:
        child_table = f"{table}__sub_{safe_table_suffix(field_name)}"
        for idx, elem in enumerate(arr):
            child_row = flatten_record_auto(elem)
            child_row["parent_id"] = parent_id
            # Synthesize an ID if the subform row has none — uses the parent
            # ID and the array index. Fivetran tolerates string IDs.
            if "ID" not in child_row:
                child_row["ID"] = f"{parent_id}__{idx}"
            upsert(child_table, child_row)

    flat = flatten_record_auto(raw, drop_keys={k for k, _ in children})
    upsert(table, flat)
    if parent_id is not None:
        current_ids.add(parent_id)


def sync_report_records(configuration: dict, state: dict,
                        workspace: str, app: dict, report: dict,
                        checkpoint_every: int = 1000):
    """Sync one report's records. Picks bulk vs incremental from state."""
    table = report["table"]
    rlink = report["link_name"]
    app_link = app["link_name"]

    cursor_key = f"{table}__last_modified"
    last_modified = state.get(cursor_key)
    full_mode = should_full_sync(state, table) or not last_modified

    if full_mode:
        _sync_full(configuration, state, workspace, app_link, rlink, table,
                   checkpoint_every=checkpoint_every)
    else:
        _sync_incremental(configuration, state, workspace, app_link, rlink, table,
                          last_modified, checkpoint_every=checkpoint_every)


def _sync_full(configuration: dict, state: dict, workspace: str, app_link: str,
               rlink: str, table: str, checkpoint_every: int):
    """Full sync. Defaults to **Data API** for completeness — Zoho's Bulk
    Read returns only the columns that the report VIEW shows, while the
    Data API with `field_config=all` returns every native field on the
    underlying form plus lookup and computed fields. For most users the
    column-completeness trade-off matters more than the speed trade-off.

    Opt into Bulk Read by setting `prefer_bulk_read: true` in configuration.
    Bulk gives ~365× the throughput on tables with tens of thousands of
    rows (one job vs hundreds of paginated GETs), but at the cost of the
    leaner column set.
    """
    # Fivetran SDK delivers all config as strings; bool("false") is True,
    # so parse explicitly.
    prefer_bulk = str(configuration.get("prefer_bulk_read") or "").strip().lower() \
        in ("true", "1", "yes", "on")
    use_bulk = prefer_bulk and not _bulk_disabled[0]
    mode = (
        "Bulk Read" if use_bulk else
        ("Data API (prefer_bulk_read=false; full columns via field_config=all)"
         if not prefer_bulk else
         "Data API (bulk disabled this run)")
    )
    log.info(f"{table}: FULL sync via {mode}")
    current_ids: set = set()
    max_modified = None
    n = 0

    def _via_data_api():
        nonlocal n, max_modified
        for raw in fetch_records(configuration, workspace, app_link, rlink):
            _process_record(raw, table, current_ids)
            mod = raw.get("Modified_Time") or raw.get("Added_Time")
            if mod and (max_modified is None or mod > max_modified):
                max_modified = mod
            n += 1
            if n % checkpoint_every == 0:
                op.checkpoint(state)
                log.fine(f"{table}: data-api checkpoint at {n} rows")

    if use_bulk:
        try:
            for raw in bulk_read(configuration, workspace, app_link, rlink):
                raw = _normalize_csv_row(raw)
                _process_record(raw, table, current_ids)
                mod = raw.get("Modified_Time") or raw.get("Added_Time")
                if mod and (max_modified is None or mod > max_modified):
                    max_modified = mod
                n += 1
                if n % checkpoint_every == 0:
                    op.checkpoint(state)
                    log.fine(f"{table}: bulk-read checkpoint at {n} rows")
            # Bulk completed cleanly → reset poll-failure streak.
            _note_bulk_poll_success()
        except ScopeMissing as e:
            # Comes from the POST step — bulk.CREATE itself is missing.
            log.warning(
                f"{table}: Bulk Read rejected at job creation for missing "
                f"scope (ZohoCreator.bulk.CREATE). Disabling Bulk Read for "
                f"the rest of this run. Detail: {e!s}"
            )
            _bulk_disabled[0] = True
            n = 0
            current_ids = set()
            max_modified = None
            _via_data_api()
        except BulkReadFailed as e:
            # Comes from poll/download — could be a genuine failure (bad job,
            # 30-min timeout) OR the bulk.READ-scope-missing pattern we see
            # often. Count it and flip the global breaker after a threshold.
            if "missing scope" in str(e):
                _note_bulk_poll_failure()
            log.warning(
                f"{table}: Bulk Read failed at poll/download "
                f"(consecutive_failures={_bulk_poll_failures[0]}); "
                f"falling back to Data API for THIS report only. "
                f"Detail: {e!s}"
            )
            n = 0
            current_ids = set()
            max_modified = None
            _via_data_api()
    else:
        _via_data_api()

    reconcile_deletes(table, current_ids, state)
    if max_modified:
        state[f"{table}__last_modified"] = max_modified
    mark_full_sync(state, table)
    op.checkpoint(state)
    log.info(f"{table}: FULL sync complete — {n} rows, max_modified={max_modified}")


def _sync_incremental(configuration: dict, state: dict, workspace: str,
                      app_link: str, rlink: str, table: str,
                      since: str, checkpoint_every: int):
    criteria = f"Modified_Time > '{since}'"
    log.info(f"{table}: INCREMENTAL sync (criteria: {criteria})")
    n = 0
    max_modified = since
    seen_this_run: set = set()
    for raw in fetch_records(configuration, workspace, app_link, rlink,
                             criteria=criteria):
        _process_record(raw, table, seen_this_run)
        mod = raw.get("Modified_Time") or raw.get("Added_Time")
        if mod and (max_modified is None or mod > max_modified):
            max_modified = mod
        n += 1
        if n % checkpoint_every == 0:
            if max_modified and max_modified != since:
                state[f"{table}__last_modified"] = max_modified
            op.checkpoint(state)
            log.fine(f"{table}: incremental checkpoint at {n} rows (cursor={max_modified})")

    if max_modified and max_modified != since:
        state[f"{table}__last_modified"] = max_modified
    op.checkpoint(state)
    log.info(f"{table}: INCREMENTAL sync complete — {n} rows, cursor={max_modified}")


def sync_all_data(configuration: dict, state: dict):
    """Iterate every report across every app in every workspace."""
    from schema import discover
    catalog = discover(configuration)

    reset_bulk_state()
    for ws in catalog["workspaces"]:
        owner = ws["owner"]
        for app in ws["apps"]:
            for report in app["reports"]:
                # Skip page-type reports — they don't expose record data.
                rtype = report.get("type")
                if isinstance(rtype, str) and "page" in rtype.lower():
                    log.fine(f"{report['table']}: skipping page-type report")
                    continue
                try:
                    sync_report_records(configuration, state, owner, app, report)
                except DailyLimitExceeded:
                    raise
                except Exception as e:
                    log.severe(
                        f"{report['table']}: failed with {e!r}; continuing with "
                        f"next report. Fivetran will retry on next sync."
                    )
                op.checkpoint(state)
