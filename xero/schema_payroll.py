"""
Schema definitions for all Xero UK Payroll API tables.

Only PK columns and columns requiring explicit types (BOOLEAN, INT, FLOAT) are
declared. All other columns are auto-inferred by the Fivetran SDK.
"""


def get_payroll_schema() -> list:
    return [
        {
            "table": "payroll_employee",
            "primary_key": ["employeeID"],
            "columns": {
                "employeeID": "STRING",
                "isOffPayrollWorker": "BOOLEAN",
            },
        },
        {
            "table": "payroll_employment",
            "primary_key": ["employeeID"],
            "columns": {
                "employeeID": "STRING",
            },
        },
        {
            "table": "payroll_employee_tax",
            "primary_key": ["employeeID"],
            "columns": {
                "employeeID": "STRING",
                "w1M1": "BOOLEAN",
                "previousTaxablePay": "FLOAT",
                "previousTaxPaid": "FLOAT",
                "isDirector": "BOOLEAN",
                "hasPostGraduateLoans": "BOOLEAN",
            },
        },
        {
            "table": "payroll_employee_opening_balance",
            "primary_key": ["employeeID"],
            "columns": {
                "employeeID": "STRING",
                "statutorySickPay": "FLOAT",
                "statutoryMaternityPay": "FLOAT",
                "statutoryPaternityPay": "FLOAT",
                "statutoryAdoptionPay": "FLOAT",
                "statutorySharedParentalPay": "FLOAT",
                "priorEmployeeNumber": "FLOAT",
            },
        },
        {
            "table": "payroll_employee_leave",
            "primary_key": ["leaveID"],
            "columns": {
                "leaveID": "STRING",
            },
        },
        {
            "table": "payroll_employee_leave_balance",
            "primary_key": ["employeeID", "leaveTypeID"],
            "columns": {
                "employeeID": "STRING",
                "leaveTypeID": "STRING",
                "balance": "FLOAT",
            },
        },
        {
            "table": "payroll_employee_statutory_leave_balance",
            "primary_key": ["employeeID", "leaveType"],
            "columns": {
                "employeeID": "STRING",
                "leaveType": "STRING",
                "balanceRemaining": "FLOAT",
            },
        },
        {
            "table": "payroll_employee_pay_template",
            "primary_key": ["payTemplateEarningID"],
            "columns": {
                "payTemplateEarningID": "STRING",
                "ratePerUnit": "FLOAT",
                "numberOfUnits": "FLOAT",
                "fixedAmount": "FLOAT",
            },
        },
        {
            "table": "payroll_salary_and_wage",
            "primary_key": ["salaryAndWagesID"],
            "columns": {
                "salaryAndWagesID": "STRING",
                "annualSalary": "FLOAT",
                "numberOfUnitsPerWeek": "FLOAT",
                "ratePerUnit": "FLOAT",
                "numberOfUnitsPerDay": "FLOAT",
            },
        },
        {
            "table": "payroll_payment_method",
            "primary_key": ["employeeID"],
            "columns": {
                "employeeID": "STRING",
            },
        },
        {
            "table": "payroll_pay_run",
            "primary_key": ["payRunID"],
            "columns": {
                "payRunID": "STRING",
                "totalCost": "FLOAT",
                "totalPay": "FLOAT",
            },
        },
        {
            "table": "payroll_payslip",
            "primary_key": ["paySlipID"],
            "columns": {
                "paySlipID": "STRING",
                "totalEarnings": "FLOAT",
                "grossEarnings": "FLOAT",
                "totalPay": "FLOAT",
                "totalEmployerTaxes": "FLOAT",
                "totalEmployeeTaxes": "FLOAT",
                "totalDeductions": "FLOAT",
                "totalReimbursements": "FLOAT",
                "totalCourtOrders": "FLOAT",
                "totalBenefits": "FLOAT",
            },
        },
        {
            "table": "payroll_payslip_earning_line",
            "primary_key": ["paySlipID", "earningsLineID"],
            "columns": {
                "paySlipID": "STRING",
                "earningsLineID": "STRING",
                "ratePerUnit": "FLOAT",
                "numberOfUnits": "FLOAT",
                "fixedAmount": "FLOAT",
                "amount": "FLOAT",
                "isLinkedToTimesheet": "BOOLEAN",
                "isAverageDailyPayRate": "BOOLEAN",
            },
        },
        {
            "table": "payroll_payslip_deduction_line",
            "primary_key": ["paySlipID", "deductionTypeID"],
            "columns": {
                "paySlipID": "STRING",
                "deductionTypeID": "STRING",
                "amount": "FLOAT",
                "subjectToTax": "BOOLEAN",
                "percentage": "FLOAT",
            },
        },
        {
            "table": "payroll_payslip_leave_accrual_line",
            "primary_key": ["paySlipID", "leaveTypeID"],
            "columns": {
                "paySlipID": "STRING",
                "leaveTypeID": "STRING",
                "numberOfUnits": "FLOAT",
            },
        },
        {
            "table": "payroll_payslip_reimbursement_line",
            "primary_key": ["paySlipID", "reimbursementTypeID"],
            "columns": {
                "paySlipID": "STRING",
                "reimbursementTypeID": "STRING",
                "amount": "FLOAT",
            },
        },
        {
            "table": "payroll_payslip_benefit_line",
            "primary_key": ["paySlipID", "benefitTypeID"],
            "columns": {
                "paySlipID": "STRING",
                "benefitTypeID": "STRING",
                "amount": "FLOAT",
                "fixedAmount": "FLOAT",
                "percentage": "FLOAT",
            },
        },
        {
            "table": "payroll_payslip_tax_line",
            "primary_key": ["paySlipID", "taxLineID"],
            "columns": {
                "paySlipID": "STRING",
                "taxLineID": "STRING",
                "isEmployerTax": "BOOLEAN",
                "amount": "FLOAT",
                "manualAdjustment": "BOOLEAN",
            },
        },
        {
            "table": "payroll_payslip_court_order_line",
            "primary_key": ["paySlipID", "courtOrderTypeID"],
            "columns": {
                "paySlipID": "STRING",
                "courtOrderTypeID": "STRING",
                "amount": "FLOAT",
            },
        },
        {
            "table": "payroll_payslip_payment_line",
            "primary_key": ["paySlipID", "paymentLineID"],
            "columns": {
                "paySlipID": "STRING",
                "paymentLineID": "STRING",
                "amount": "FLOAT",
            },
        },
        {
            "table": "payroll_leave_type",
            "primary_key": ["leaveTypeID"],
            "columns": {
                "leaveTypeID": "STRING",
                "isPaidLeave": "BOOLEAN",
                "showOnPayslip": "BOOLEAN",
                "isStatutoryLeave": "BOOLEAN",
                "isActive": "BOOLEAN",
            },
        },
        {
            "table": "payroll_earning_rate",
            "primary_key": ["earningsRateID"],
            "columns": {
                "earningsRateID": "STRING",
                "currentRecord": "BOOLEAN",
                "fixedAmount": "FLOAT",
                "ratePerUnit": "FLOAT",
                "multipleOfOrdinaryEarningsRate": "FLOAT",
                "excludedFromMinimumWage": "BOOLEAN",
            },
        },
        {
            "table": "payroll_deduction",
            "primary_key": ["deductionId"],
            "columns": {
                "deductionId": "STRING",
                "currentRecord": "BOOLEAN",
                "standardAmount": "FLOAT",
                "percentage": "FLOAT",
                "reducesSuperLiability": "BOOLEAN",
                "reducesTaxLiability": "BOOLEAN",
                "subjectToNIC": "BOOLEAN",
                "subjectToTax": "BOOLEAN",
                "isReducedByBasicRate": "BOOLEAN",
                "applyToPensionCalculations": "BOOLEAN",
                "isCalculatingOnQualifyingEarnings": "BOOLEAN",
                "isPension": "BOOLEAN",
                "excludedFromMinimumWage": "BOOLEAN",
            },
        },
        {
            "table": "payroll_benefit",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "standardAmount": "FLOAT",
                "percentage": "FLOAT",
                "currentRecord": "BOOLEAN",
                "showBalanceToEmployee": "BOOLEAN",
                "subjectToNIC": "BOOLEAN",
                "subjectToPension": "BOOLEAN",
                "subjectToTax": "BOOLEAN",
                "isCalculatingOnQualifyingEarnings": "BOOLEAN",
            },
        },
        {
            "table": "payroll_reimbursement",
            "primary_key": ["reimbursementID"],
            "columns": {
                "reimbursementID": "STRING",
                "currentRecord": "BOOLEAN",
            },
        },
        {
            "table": "payroll_timesheet",
            "primary_key": ["timesheetID"],
            "columns": {
                "timesheetID": "STRING",
                "totalHours": "FLOAT",
            },
        },
        {
            "table": "payroll_timesheet_line",
            "primary_key": ["timesheetID", "timesheetLineID"],
            "columns": {
                "timesheetID": "STRING",
                "timesheetLineID": "STRING",
                "numberOfUnits": "FLOAT",
            },
        },
        {
            "table": "payroll_earnings_order",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "currentRecord": "BOOLEAN",
            },
        },
        {
            "table": "payroll_settings",
            "primary_key": ["_singleton"],
            "columns": {
                "_singleton": "STRING",
            },
        },
    ]
