"""
Sync logic for all Xero UK Payroll API entities.

Each sync function calls op.upsert() directly (no yield)
and mutates the state dict for cursor tracking.
"""

import json
from datetime import datetime, timezone

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import PAYROLL_BASE, fetch_all_pages, fetch_single
from helpers import upsert as _upsert


def _flatten_nested(record, keys):
    """Convert nested dicts/lists to JSON strings."""
    for key in keys:
        if key in record and not isinstance(record[key], str):
            record[key] = json.dumps(record[key])


# ── Reference Data ───────────────────────────────────────────────────────────


def sync_leave_types(config, state):
    data = fetch_single(config, "/LeaveTypes", base_url=PAYROLL_BASE)
    for record in data.get("leaveTypes", []):
        _upsert(table="payroll_leave_type", data=record)


def sync_earning_rates(config, state):
    data = fetch_single(config, "/EarningsRates", base_url=PAYROLL_BASE)
    for record in data.get("earningsRates", []):
        _upsert(table="payroll_earning_rate", data=record)


def sync_deductions(config, state):
    data = fetch_single(config, "/Deductions", base_url=PAYROLL_BASE)
    for record in data.get("deductions", []):
        _upsert(table="payroll_deduction", data=record)


def sync_benefits(config, state):
    data = fetch_single(config, "/Benefits", base_url=PAYROLL_BASE)
    for record in data.get("benefits", []):
        _upsert(table="payroll_benefit", data=record)


def sync_reimbursements(config, state):
    data = fetch_single(config, "/Reimbursements", base_url=PAYROLL_BASE)
    for record in data.get("reimbursements", []):
        _upsert(table="payroll_reimbursement", data=record)


def sync_settings(config, state):
    data = fetch_single(config, "/Settings", base_url=PAYROLL_BASE)
    settings = data.get("settings", {})
    if settings:
        settings["_singleton"] = "settings"
        _flatten_nested(settings, ["accounts"])
        _upsert(table="payroll_settings", data=settings)


# ── Employees + Sub-resources ────────────────────────────────────────────────


def _sync_single_employee_subresources(config, emp_id):
    """Sync all sub-resources for a single employee."""

    # Employment
    try:
        emp_data = fetch_single(config, f"/Employees/{emp_id}/Employment", base_url=PAYROLL_BASE)
        employment = emp_data.get("employment")
        if employment:
            employment["employeeID"] = emp_id
            _upsert(table="payroll_employment", data=employment)
    except Exception as e:
        log.warning(f"Could not fetch employment for {emp_id}: {e}")

    # Tax
    try:
        tax_data = fetch_single(config, f"/Employees/{emp_id}/Tax", base_url=PAYROLL_BASE)
        tax = tax_data.get("employeeTax")
        if tax:
            tax["employeeID"] = emp_id
            _upsert(table="payroll_employee_tax", data=tax)
    except Exception as e:
        log.warning(f"Could not fetch tax for {emp_id}: {e}")

    # Opening Balances
    try:
        ob_data = fetch_single(config, f"/Employees/{emp_id}/ukopeningbalances", base_url=PAYROLL_BASE)
        ob = ob_data.get("openingBalances")
        if ob:
            if isinstance(ob, dict):
                ob["employeeID"] = emp_id
                _upsert(table="payroll_employee_opening_balance", data=ob)
            elif isinstance(ob, list):
                for bal in ob:
                    bal["employeeID"] = emp_id
                    _upsert(table="payroll_employee_opening_balance", data=bal)
    except Exception as e:
        log.warning(f"Could not fetch opening balances for {emp_id}: {e}")

    # Leave
    try:
        leaves = fetch_all_pages(
            config, f"/Employees/{emp_id}/Leave", "leave", base_url=PAYROLL_BASE,
        )
        for leave in leaves:
            leave["employeeID"] = emp_id
            _flatten_nested(leave, ["periods"])
            _upsert(table="payroll_employee_leave", data=leave)
    except Exception as e:
        log.warning(f"Could not fetch leave for {emp_id}: {e}")

    # Leave Balances
    try:
        lb_data = fetch_single(config, f"/Employees/{emp_id}/LeaveBalances", base_url=PAYROLL_BASE)
        for bal in lb_data.get("leaveBalances", []):
            bal["employeeID"] = emp_id
            _upsert(table="payroll_employee_leave_balance", data=bal)
    except Exception as e:
        log.warning(f"Could not fetch leave balances for {emp_id}: {e}")

    # Statutory Leave Balance
    try:
        slb_data = fetch_single(
            config, f"/Employees/{emp_id}/StatutoryLeaveBalance", base_url=PAYROLL_BASE,
        )
        for bal in slb_data.get("statutoryLeaveBalance", []):
            bal["employeeID"] = emp_id
            _upsert(table="payroll_employee_statutory_leave_balance", data=bal)
    except Exception as e:
        log.warning(f"Could not fetch statutory leave balance for {emp_id}: {e}")

    # Pay Templates
    try:
        pt_data = fetch_single(config, f"/Employees/{emp_id}/PayTemplates", base_url=PAYROLL_BASE)
        for template in pt_data.get("earningTemplates", []):
            template["employeeID"] = emp_id
            _upsert(table="payroll_employee_pay_template", data=template)
    except Exception as e:
        log.warning(f"Could not fetch pay templates for {emp_id}: {e}")

    # Salary and Wages
    try:
        salaries = fetch_all_pages(
            config, f"/Employees/{emp_id}/SalaryAndWages", "salaryAndWages",
            base_url=PAYROLL_BASE,
        )
        for sal in salaries:
            sal["employeeID"] = emp_id
            _upsert(table="payroll_salary_and_wage", data=sal)
    except Exception as e:
        log.warning(f"Could not fetch salary and wages for {emp_id}: {e}")

    # Payment Methods
    try:
        pm_data = fetch_single(
            config, f"/Employees/{emp_id}/PaymentMethods", base_url=PAYROLL_BASE,
        )
        pm = pm_data.get("paymentMethod")
        if pm:
            pm["employeeID"] = emp_id
            _flatten_nested(pm, ["bankAccounts"])
            _upsert(table="payroll_payment_method", data=pm)
    except Exception as e:
        log.warning(f"Could not fetch payment methods for {emp_id}: {e}")


def sync_employees(config, state):
    """
    Sync all payroll employees and their sub-resources.
    Checkpoints after each employee to allow resumption.

    On incremental runs, only fetches sub-resources for employees whose
    updatedDateUtc is newer than the last full sync timestamp. This saves
    ~9 API calls per unchanged employee.
    """
    employees = fetch_all_pages(
        config, "/Employees", "employees", base_url=PAYROLL_BASE,
    )

    batch_idx_key = "payroll_employee_batch_idx"
    last_synced_idx = int(state.get(batch_idx_key, 0))
    last_emp_sync = state.get("payroll_employee_last_sync", "")

    for idx, emp in enumerate(employees):
        if idx < last_synced_idx:
            continue

        emp_id = emp.get("employeeID", "")
        updated = emp.get("updatedDateUtc", "")

        _flatten_nested(emp, ["address"])
        _upsert(table="payroll_employee", data=emp)

        # Only fetch sub-resources if employee changed since last sync
        if not last_emp_sync or (updated and updated > last_emp_sync):
            log.info(f"Syncing payroll sub-resources for employee {idx + 1}: {emp_id}")
            _sync_single_employee_subresources(config, emp_id)
        else:
            log.fine(f"Skipping sub-resources for unchanged employee {emp_id}")

        state[batch_idx_key] = str(idx + 1)
        op.checkpoint(state)

    # Reset batch index for next full sync
    state[batch_idx_key] = "0"
    # Save timestamp so next run can skip unchanged employees
    state["payroll_employee_last_sync"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Pay Runs & Payslips ──────────────────────────────────────────────────────


def sync_pay_runs(config, state):
    pay_runs = list(fetch_all_pages(config, "/PayRuns", "payRuns", base_url=PAYROLL_BASE))
    pay_run_ids = []
    for pr in pay_runs:
        pr.pop("paySlips", None)  # payslips synced separately
        pay_run_ids.append(pr.get("payRunID", ""))
        _upsert(table="payroll_pay_run", data=pr)
    # Cache IDs so sync_payslips can skip re-fetching /PayRuns
    state["_payrun_ids_cache"] = json.dumps(pay_run_ids)


def _process_payslip(payslip):
    """Extract child line arrays from a payslip and upsert everything."""
    payslip_id = payslip.get("paySlipID", "")
    if not payslip_id:
        return

    line_types = {
        "earningsLines": "payroll_payslip_earning_line",
        "deductionLines": "payroll_payslip_deduction_line",
        "leaveAccrualLines": "payroll_payslip_leave_accrual_line",
        "reimbursementLines": "payroll_payslip_reimbursement_line",
        "benefitLines": "payroll_payslip_benefit_line",
        "taxLines": "payroll_payslip_tax_line",
        "courtOrderLines": "payroll_payslip_court_order_line",
        "paymentLines": "payroll_payslip_payment_line",
    }

    for json_key, table_name in line_types.items():
        lines = payslip.pop(json_key, [])
        for line_idx, line in enumerate(lines):
            line["paySlipID"] = payslip_id
            id_field = json_key.replace("Lines", "LineID").replace("lines", "LineID")
            if id_field not in line:
                line[id_field] = str(line_idx)
            _upsert(table=table_name, data=line)

    _upsert(table="payroll_payslip", data=payslip)


def sync_payslips(config, state):
    """
    Fetch payslips in batch per pay run using GET /Payslips?PayRunID=xxx
    instead of fetching each payslip individually. Saves hundreds of API calls.

    Uses cached pay run IDs from sync_pay_runs() to avoid re-fetching /PayRuns.
    Falls back to a fresh fetch if the cache is missing (e.g., resumed from
    a checkpoint between sync_pay_runs and sync_payslips).
    """
    cached = state.pop("_payrun_ids_cache", None)
    if cached:
        pay_run_ids = json.loads(cached)
    else:
        # Fallback: re-fetch if cache missing
        pay_runs = fetch_all_pages(config, "/PayRuns", "payRuns", base_url=PAYROLL_BASE)
        pay_run_ids = [pr.get("payRunID", "") for pr in pay_runs]

    payslip_batch_key = "payroll_payslip_payrun_idx"
    last_synced_run = int(state.get(payslip_batch_key, 0))

    for run_idx, pay_run_id in enumerate(pay_run_ids, start=1):
        if run_idx <= last_synced_run:
            continue
        if not pay_run_id:
            continue

        try:
            # Batch fetch: all payslips for this pay run in paginated calls
            payslips = fetch_all_pages(
                config, f"/Payslips?PayRunID={pay_run_id}",
                "paySlips", base_url=PAYROLL_BASE,
            )
            for payslip in payslips:
                _process_payslip(payslip)
        except Exception as e:
            log.warning(f"Could not fetch payslips for pay run {pay_run_id}: {e}")

        state[payslip_batch_key] = str(run_idx)
        op.checkpoint(state)

    # Reset for next sync
    state[payslip_batch_key] = "0"


# ── Timesheets ───────────────────────────────────────────────────────────────


def sync_timesheets(config, state):
    timesheets = fetch_all_pages(config, "/Timesheets", "timesheets", base_url=PAYROLL_BASE)
    for ts in timesheets:
        ts_id = ts.get("timesheetID", "")
        lines = ts.pop("timesheetLines", [])

        _upsert(table="payroll_timesheet", data=ts)

        for idx, line in enumerate(lines):
            line["timesheetID"] = ts_id
            if "timesheetLineID" not in line:
                line["timesheetLineID"] = str(idx)
            _flatten_nested(line, ["timesheetEarningsLines"])
            _upsert(table="payroll_timesheet_line", data=line)


# ── Earnings Orders ──────────────────────────────────────────────────────────


def sync_earnings_orders(config, state):
    orders = fetch_all_pages(config, "/EarningsOrders", "earningsOrders", base_url=PAYROLL_BASE)
    for order in orders:
        _upsert(table="payroll_earnings_order", data=order)


# ── Sync order ───────────────────────────────────────────────────────────────

PAYROLL_REFERENCE_SYNCS = [
    sync_leave_types,
    sync_earning_rates,
    sync_deductions,
    sync_benefits,
    sync_reimbursements,
    sync_settings,
]

PAYROLL_DATA_SYNCS = [
    sync_employees,
    sync_pay_runs,
    sync_payslips,
    sync_timesheets,
    sync_earnings_orders,
]
