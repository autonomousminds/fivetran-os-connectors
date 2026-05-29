"""
Schema definitions for Xero Reports API tables.

Two patterns:
  - `accounting_report_trial_balance` — dedicated table for the most-used report
    (used for account-balance reconciliation and answering balance questions).
  - `accounting_report_row` — generic cell-level table for every other report
    (BalanceSheet, ProfitAndLoss, BankSummary, ExecutiveSummary, AgedPayables,
    AgedReceivables, BudgetSummary, TaxReports, 1099). One row per cell, so
    analysts can pivot to whatever shape they need.

Each granular Reports scope was added to ACCOUNTING_SCOPES in auth.py and the
endpoints all live on the same Accounting API base URL — no separate token
needed.
"""


def get_reports_schema() -> list:
    return [
        # Dedicated Trial Balance: known column shape, account-level granularity.
        {
            "table": "accounting_report_trial_balance",
            "primary_key": ["ReportDate", "RowIndex"],
            "columns": {
                "ReportDate": "STRING",
                "RowIndex": "INT",
                "Section": "STRING",
                "RowType": "STRING",
                "AccountID": "STRING",
                "AccountName": "STRING",
                "Debit": "FLOAT",
                "Credit": "FLOAT",
                "YTDDebit": "FLOAT",
                "YTDCredit": "FLOAT",
                "SyncTimestamp": "STRING",
            },
        },
        # Generic cell-level table for every other report.
        {
            "table": "accounting_report_row",
            "primary_key": ["ReportType", "ReportDate", "RowIndex", "CellIndex"],
            "columns": {
                "ReportType": "STRING",
                "ReportDate": "STRING",
                "RowIndex": "INT",
                "CellIndex": "INT",
                "Section": "STRING",
                "RowType": "STRING",
                "CellValue": "STRING",
                "CellAccountID": "STRING",
                "SyncTimestamp": "STRING",
            },
        },
        # Report header / metadata
        {
            "table": "accounting_report_run",
            "primary_key": ["ReportType", "ReportDate"],
            "columns": {
                "ReportType": "STRING",
                "ReportDate": "STRING",
                "ReportName": "STRING",
                "ReportTitle": "STRING",
                "UpdatedDateUTC": "STRING",
                "SyncTimestamp": "STRING",
            },
        },
    ]
