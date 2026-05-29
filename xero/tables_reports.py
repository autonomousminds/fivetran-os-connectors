"""
Sync logic for Xero Accounting Reports.

The Reports API returns a hierarchical Rows/Cells structure that's a poor fit
for tabular storage as-is. We flatten:

  - Trial Balance → dedicated `accounting_report_trial_balance` table with
    typed Debit/Credit/YTD columns and AccountID lifted out of cell attributes.
    This is the canonical source for account-balance queries.
  - All other reports → generic `accounting_report_row` table with one row per
    cell. PK is (ReportType, ReportDate, RowIndex, CellIndex). Plus an
    `accounting_report_run` header row per (ReportType, ReportDate).

Each report sync is a single GET — these endpoints don't paginate or expose
If-Modified-Since. We re-fetch on every sync, so the report tables show
historical snapshots if syncs run daily.

Scope: every report uses its specific granular scope (e.g.
accounting.reports.trialbalance.read for TrialBalance). All are bundled into
the 'accounting' token in auth.py — denied scopes are dropped by scope
probing, and the corresponding report sync will fail at the API call.
We catch and warn rather than aborting the whole sync.
"""

import datetime as _dt

from fivetran_connector_sdk import Logging as log

from api_client import ACCOUNTING_BASE, api_request
from helpers import upsert as _upsert


# Reports we know how to fetch in a single call. Aged reports
# (AgedPayablesByContact, AgedReceivablesByContact) are intentionally excluded:
# Xero requires a mandatory `contactId` query param so they must be called
# per-contact, not bulk. Adding per-contact aged reporting later is a separate
# piece of work (N API calls = N contacts).
_GENERIC_REPORTS = [
    ("BalanceSheet",     "balancesheet"),
    ("ProfitAndLoss",    "profitandloss"),
    ("BankSummary",      "banksummary"),
    ("ExecutiveSummary", "executivesummary"),
    ("BudgetSummary",    "budgetsummary"),
]


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_float(s):
    """Xero report cells are strings; convert numeric ones to float."""
    if s is None or s == "":
        return None
    try:
        # Xero uses parentheses for negatives in some locales; handle both.
        v = str(s).replace(",", "").strip()
        if v.startswith("(") and v.endswith(")"):
            v = "-" + v[1:-1]
        return float(v)
    except (ValueError, TypeError):
        return None


def _cell_account_id(cell: dict) -> str:
    """Extract the AccountID attribute from a report cell, if present."""
    for attr in cell.get("Attributes", []) or []:
        if attr.get("Id") == "account":
            return attr.get("Value", "") or ""
    return ""


def _fetch_report(config: dict, report_endpoint: str) -> dict:
    """GET /Reports/{endpoint}. Returns the first Reports[] entry or None on failure."""
    url = f"{ACCOUNTING_BASE}/Reports/{report_endpoint}"
    try:
        data = api_request(config, url, scope_group="accounting")
    except Exception as e:
        log.warning(f"Failed to fetch report {report_endpoint}: {e}")
        return None
    reports = data.get("Reports", [])
    return reports[0] if reports else None


def _upsert_report_run(report: dict, report_type: str, report_date: str, now: str):
    _upsert("accounting_report_run", {
        "ReportType":     report_type,
        "ReportDate":     report_date,
        "ReportName":     report.get("ReportName", ""),
        "ReportTitle":    "; ".join(report.get("ReportTitles", []) or []),
        "UpdatedDateUTC": report.get("UpdatedDateUTC", ""),
        "SyncTimestamp":  now,
    })


def _flatten_rows(rows: list, section: str = ""):
    """Yield (row_index, section, row_type, cells) for every leaf row.
    Section rows can nest other rows under a 'Rows' key.
    Section rows themselves are yielded too (with empty cells) so totals/labels
    survive into the output."""
    idx = 0
    stack = [(rows, section)]
    while stack:
        current_rows, current_section = stack.pop(0)
        for row in current_rows:
            row_type = row.get("RowType", "")
            if row_type == "Section":
                # Emit a section marker row, then recurse into its rows
                sect_title = row.get("Title", "") or current_section
                yield idx, sect_title, row_type, row.get("Cells", []) or []
                idx += 1
                if row.get("Rows"):
                    stack.insert(0, (row["Rows"], sect_title))
            else:
                yield idx, current_section, row_type, row.get("Cells", []) or []
                idx += 1


def sync_trial_balance(config, state):
    """Sync the Trial Balance report into the dedicated typed table."""
    now = _now_iso()
    report = _fetch_report(config, "TrialBalance")
    if not report:
        return

    report_date = report.get("ReportDate", "") or now[:10]
    _upsert_report_run(report, "TrialBalance", report_date, now)

    count = 0
    for idx, section, row_type, cells in _flatten_rows(report.get("Rows", []) or []):
        if not cells and row_type != "Section":
            continue
        # Cells: [Account, Debit, Credit, YTD Debit, YTD Credit]
        account_name = cells[0].get("Value", "") if len(cells) > 0 else ""
        account_id   = _cell_account_id(cells[0]) if len(cells) > 0 else ""
        debit        = _to_float(cells[1].get("Value")) if len(cells) > 1 else None
        credit       = _to_float(cells[2].get("Value")) if len(cells) > 2 else None
        ytd_debit    = _to_float(cells[3].get("Value")) if len(cells) > 3 else None
        ytd_credit   = _to_float(cells[4].get("Value")) if len(cells) > 4 else None

        _upsert("accounting_report_trial_balance", {
            "ReportDate":   report_date,
            "RowIndex":     idx,
            "Section":      section,
            "RowType":      row_type,
            "AccountID":    account_id,
            "AccountName":  account_name,
            "Debit":        debit,
            "Credit":       credit,
            "YTDDebit":     ytd_debit,
            "YTDCredit":    ytd_credit,
            "SyncTimestamp": now,
        })
        count += 1
    log.info(f"Trial Balance: {count} rows synced (report date: {report_date})")


def _sync_generic_report(config, report_endpoint: str):
    """Sync any report into the generic accounting_report_row table."""
    now = _now_iso()
    report = _fetch_report(config, report_endpoint)
    if not report:
        return
    report_type = report.get("ReportType", report_endpoint)
    report_date = report.get("ReportDate", "") or now[:10]
    _upsert_report_run(report, report_type, report_date, now)

    count = 0
    for row_idx, section, row_type, cells in _flatten_rows(report.get("Rows", []) or []):
        for cell_idx, cell in enumerate(cells):
            _upsert("accounting_report_row", {
                "ReportType":     report_type,
                "ReportDate":     report_date,
                "RowIndex":       row_idx,
                "CellIndex":      cell_idx,
                "Section":        section,
                "RowType":        row_type,
                "CellValue":      str(cell.get("Value", "")) if cell.get("Value") is not None else "",
                "CellAccountID":  _cell_account_id(cell),
                "SyncTimestamp":  now,
            })
            count += 1
    log.info(f"Report {report_type}: {count} cells synced (report date: {report_date})")


def _make_sync(endpoint, label):
    def sync(config, state):
        _sync_generic_report(config, endpoint)
    sync.__name__ = f"sync_report_{label}_{endpoint.lower()}"
    return sync


REPORT_SYNCS = [sync_trial_balance] + [
    _make_sync(endpoint, label) for endpoint, label in _GENERIC_REPORTS
]
