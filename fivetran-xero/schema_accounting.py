"""
Schema definitions for all Xero Accounting API tables.

Only PK columns and columns requiring explicit types (BOOLEAN, INT, FLOAT) are
declared. All other columns are auto-inferred by the Fivetran SDK, which also
means new fields added by Xero are picked up automatically.
"""


def get_accounting_schema() -> list:
    return [
        # ── Organisation & Settings ──────────────────────────────────────
        {
            "table": "accounting_organisation",
            "primary_key": ["OrganisationID"],
            "columns": {
                "OrganisationID": "STRING",
                "PaysTax": "BOOLEAN",
                "IsDemoCompany": "BOOLEAN",
                "FinancialYearEndDay": "INT",
                "FinancialYearEndMonth": "INT",
            },
        },
        {
            "table": "accounting_settings",
            "primary_key": ["_singleton"],
            "columns": {
                "_singleton": "STRING",
                "FinancialYearEndDay": "INT",
                "FinancialYearEndMonth": "INT",
            },
        },
        # ── Accounts ─────────────────────────────────────────────────────
        {
            "table": "accounting_account",
            "primary_key": ["AccountID"],
            "columns": {
                "AccountID": "STRING",
                "EnablePaymentsToAccount": "BOOLEAN",
                "ShowInExpenseClaims": "BOOLEAN",
                "HasAttachments": "BOOLEAN",
                "AddToWatchlist": "BOOLEAN",
            },
        },
        # ── Assets ───────────────────────────────────────────────────────
        {
            "table": "accounting_asset",
            "primary_key": ["AssetId"],
            "columns": {
                "AssetId": "STRING",
                "PurchasePrice": "FLOAT",
                "DisposalPrice": "FLOAT",
                "AccountingBookValue": "FLOAT",
                "CanRollback": "BOOLEAN",
            },
        },
        {
            "table": "accounting_asset_type",
            "primary_key": ["AssetTypeId"],
            "columns": {
                "AssetTypeId": "STRING",
                "Locks": "LONG",
            },
        },
        # ── Bank Transactions ────────────────────────────────────────────
        {
            "table": "accounting_bank_transaction",
            "primary_key": ["BankTransactionID"],
            "columns": {
                "BankTransactionID": "STRING",
                "IsReconciled": "BOOLEAN",
                "HasAttachments": "BOOLEAN",
                "SubTotal": "FLOAT",
                "TotalTax": "FLOAT",
                "Total": "FLOAT",
                "CurrencyRate": "FLOAT",
                "BatchPaymentTotalAmount": "FLOAT",
                "BatchPaymentIsReconciled": "BOOLEAN",
            },
        },
        {
            "table": "accounting_bank_transaction_line_item",
            "primary_key": ["BankTransactionID", "LineItemID"],
            "columns": {
                "BankTransactionID": "STRING",
                "LineItemID": "STRING",
                "Quantity": "FLOAT",
                "UnitAmount": "FLOAT",
                "TaxAmount": "FLOAT",
                "LineAmount": "FLOAT",
                "DiscountRate": "FLOAT",
            },
        },
        # ── Bank Transfers ───────────────────────────────────────────────
        {
            "table": "accounting_bank_transfer",
            "primary_key": ["BankTransferID"],
            "columns": {
                "BankTransferID": "STRING",
                "Amount": "FLOAT",
                "CurrencyRate": "FLOAT",
                "FromIsReconciled": "BOOLEAN",
                "ToIsReconciled": "BOOLEAN",
            },
        },
        # ── Batch Payments ───────────────────────────────────────────────
        {
            "table": "accounting_batch_payment",
            "primary_key": ["BatchPaymentID"],
            "columns": {
                "BatchPaymentID": "STRING",
                "TotalAmount": "FLOAT",
                "IsReconciled": "BOOLEAN",
            },
        },
        # ── Branding Themes ──────────────────────────────────────────────
        {
            "table": "accounting_branding_theme",
            "primary_key": ["BrandingThemeID"],
            "columns": {
                "BrandingThemeID": "STRING",
                "SortOrder": "INT",
            },
        },
        # ── Contacts ─────────────────────────────────────────────────────
        {
            "table": "accounting_contact",
            "primary_key": ["ContactID"],
            "columns": {
                "ContactID": "STRING",
                "IsSupplier": "BOOLEAN",
                "IsCustomer": "BOOLEAN",
                "Discount": "FLOAT",
                "HasAttachments": "BOOLEAN",
                "HasValidationErrors": "BOOLEAN",
            },
        },
        {
            "table": "accounting_contact_address",
            "primary_key": ["ContactID", "AddressType"],
            "columns": {
                "ContactID": "STRING",
                "AddressType": "STRING",
            },
        },
        {
            "table": "accounting_contact_group",
            "primary_key": ["ContactGroupID"],
            "columns": {
                "ContactGroupID": "STRING",
            },
        },
        {
            "table": "accounting_contact_group_member",
            "primary_key": ["ContactGroupID", "ContactID"],
            "columns": {
                "ContactGroupID": "STRING",
                "ContactID": "STRING",
            },
        },
        # ── Credit Notes ─────────────────────────────────────────────────
        {
            "table": "accounting_credit_note",
            "primary_key": ["CreditNoteID"],
            "columns": {
                "CreditNoteID": "STRING",
                "SentToContact": "BOOLEAN",
                "HasAttachments": "BOOLEAN",
                "HasErrors": "BOOLEAN",
                "SubTotal": "FLOAT",
                "TotalTax": "FLOAT",
                "Total": "FLOAT",
                "CISDeduction": "FLOAT",
                "CISRate": "FLOAT",
                "RemainingCredit": "FLOAT",
                "AppliedAmount": "FLOAT",
                "CurrencyRate": "FLOAT",
            },
        },
        {
            "table": "accounting_credit_note_line_item",
            "primary_key": ["CreditNoteID", "LineItemID"],
            "columns": {
                "CreditNoteID": "STRING",
                "LineItemID": "STRING",
                "Quantity": "FLOAT",
                "UnitAmount": "FLOAT",
                "TaxAmount": "FLOAT",
                "LineAmount": "FLOAT",
                "DiscountRate": "FLOAT",
            },
        },
        {
            "table": "accounting_credit_note_line_item_tracking",
            "primary_key": ["CreditNoteID", "LineItemID", "TrackingCategoryID"],
            "columns": {
                "CreditNoteID": "STRING",
                "LineItemID": "STRING",
                "TrackingCategoryID": "STRING",
            },
        },
        # ── Allocations ──────────────────────────────────────────────────
        {
            "table": "accounting_allocation",
            "primary_key": ["SourceID", "InvoiceID", "Index"],
            "columns": {
                "SourceID": "STRING",
                "InvoiceID": "STRING",
                "Index": "STRING",
                "Amount": "FLOAT",
                "IsDeleted": "BOOLEAN",
            },
        },
        # ── Currencies ───────────────────────────────────────────────────
        {
            "table": "accounting_currency",
            "primary_key": ["Code"],
            "columns": {
                "Code": "STRING",
            },
        },
        # ── Employees (Accounting API — limited fields) ────────────────
        {
            "table": "accounting_employee",
            "primary_key": ["EmployeeID"],
            "columns": {
                "EmployeeID": "STRING",
            },
        },
        # ── Expense Claims ───────────────────────────────────────────────
        {
            "table": "accounting_expense_claim",
            "primary_key": ["ExpenseClaimID"],
            "columns": {
                "ExpenseClaimID": "STRING",
                "Total": "FLOAT",
                "AmountDue": "FLOAT",
                "AmountPaid": "FLOAT",
            },
        },
        # ── Invoices ─────────────────────────────────────────────────────
        {
            "table": "accounting_invoice",
            "primary_key": ["InvoiceID"],
            "columns": {
                "InvoiceID": "STRING",
                "SentToContact": "BOOLEAN",
                "IsDiscounted": "BOOLEAN",
                "HasAttachments": "BOOLEAN",
                "HasErrors": "BOOLEAN",
                "SubTotal": "FLOAT",
                "TotalTax": "FLOAT",
                "Total": "FLOAT",
                "TotalDiscount": "FLOAT",
                "AmountDue": "FLOAT",
                "AmountPaid": "FLOAT",
                "AmountCredited": "FLOAT",
                "CISDeduction": "FLOAT",
                "CISRate": "FLOAT",
                "CurrencyRate": "FLOAT",
            },
        },
        {
            "table": "accounting_invoice_line_item",
            "primary_key": ["InvoiceID", "LineItemID"],
            "columns": {
                "InvoiceID": "STRING",
                "LineItemID": "STRING",
                "Quantity": "FLOAT",
                "UnitAmount": "FLOAT",
                "TaxAmount": "FLOAT",
                "LineAmount": "FLOAT",
                "DiscountRate": "FLOAT",
                "DiscountAmount": "FLOAT",
            },
        },
        {
            "table": "accounting_invoice_line_item_tracking",
            "primary_key": ["InvoiceID", "LineItemID", "TrackingCategoryID"],
            "columns": {
                "InvoiceID": "STRING",
                "LineItemID": "STRING",
                "TrackingCategoryID": "STRING",
            },
        },
        # ── Items ────────────────────────────────────────────────────────
        {
            "table": "accounting_item",
            "primary_key": ["ItemID"],
            "columns": {
                "ItemID": "STRING",
                "IsSold": "BOOLEAN",
                "IsPurchased": "BOOLEAN",
                "IsTrackedAsInventory": "BOOLEAN",
                "TotalCostPool": "FLOAT",
                "QuantityOnHand": "FLOAT",
                "PurchaseUnitPrice": "FLOAT",
                "SalesUnitPrice": "FLOAT",
            },
        },
        # ── Journals ─────────────────────────────────────────────────────
        {
            "table": "accounting_journal",
            "primary_key": ["JournalID"],
            "columns": {
                "JournalID": "STRING",
                "JournalNumber": "INT",
            },
        },
        {
            "table": "accounting_journal_line",
            "primary_key": ["JournalID", "JournalLineID"],
            "columns": {
                "JournalID": "STRING",
                "JournalLineID": "STRING",
                "NetAmount": "FLOAT",
                "GrossAmount": "FLOAT",
                "TaxAmount": "FLOAT",
            },
        },
        {
            "table": "accounting_journal_line_tracking",
            "primary_key": ["JournalID", "JournalLineID", "TrackingCategoryID"],
            "columns": {
                "JournalID": "STRING",
                "JournalLineID": "STRING",
                "TrackingCategoryID": "STRING",
            },
        },
        # ── Linked Transactions ──────────────────────────────────────────
        {
            "table": "accounting_linked_transaction",
            "primary_key": ["LinkedTransactionID"],
            "columns": {
                "LinkedTransactionID": "STRING",
            },
        },
        # ── Manual Journals ──────────────────────────────────────────────
        {
            "table": "accounting_manual_journal",
            "primary_key": ["ManualJournalID"],
            "columns": {
                "ManualJournalID": "STRING",
                "ShowOnCashBasisReports": "BOOLEAN",
                "HasAttachments": "BOOLEAN",
            },
        },
        {
            "table": "accounting_manual_journal_line",
            "primary_key": ["ManualJournalID", "LineItemID"],
            "columns": {
                "ManualJournalID": "STRING",
                "LineItemID": "STRING",
                "LineAmount": "FLOAT",
                "TaxAmount": "FLOAT",
                "IsBlank": "BOOLEAN",
            },
        },
        # ── Overpayments ─────────────────────────────────────────────────
        {
            "table": "accounting_overpayment",
            "primary_key": ["OverpaymentID"],
            "columns": {
                "OverpaymentID": "STRING",
                "SubTotal": "FLOAT",
                "TotalTax": "FLOAT",
                "Total": "FLOAT",
                "CurrencyRate": "FLOAT",
                "RemainingCredit": "FLOAT",
                "AppliedAmount": "FLOAT",
                "HasAttachments": "BOOLEAN",
            },
        },
        {
            "table": "accounting_overpayment_line_item",
            "primary_key": ["OverpaymentID", "LineItemID"],
            "columns": {
                "OverpaymentID": "STRING",
                "LineItemID": "STRING",
                "Quantity": "FLOAT",
                "UnitAmount": "FLOAT",
                "TaxAmount": "FLOAT",
                "LineAmount": "FLOAT",
            },
        },
        # ── Payments ─────────────────────────────────────────────────────
        {
            "table": "accounting_payment",
            "primary_key": ["PaymentID"],
            "columns": {
                "PaymentID": "STRING",
                "Amount": "FLOAT",
                "BankAmount": "FLOAT",
                "CurrencyRate": "FLOAT",
                "IsReconciled": "BOOLEAN",
                "HasAccount": "BOOLEAN",
                "HasValidationErrors": "BOOLEAN",
            },
        },
        # ── Prepayments ──────────────────────────────────────────────────
        {
            "table": "accounting_prepayment",
            "primary_key": ["PrepaymentID"],
            "columns": {
                "PrepaymentID": "STRING",
                "SubTotal": "FLOAT",
                "TotalTax": "FLOAT",
                "Total": "FLOAT",
                "CurrencyRate": "FLOAT",
                "RemainingCredit": "FLOAT",
                "AppliedAmount": "FLOAT",
                "HasAttachments": "BOOLEAN",
            },
        },
        {
            "table": "accounting_prepayment_line_item",
            "primary_key": ["PrepaymentID", "LineItemID"],
            "columns": {
                "PrepaymentID": "STRING",
                "LineItemID": "STRING",
                "Quantity": "FLOAT",
                "UnitAmount": "FLOAT",
                "TaxAmount": "FLOAT",
                "LineAmount": "FLOAT",
            },
        },
        # ── Purchase Orders ──────────────────────────────────────────────
        {
            "table": "accounting_purchase_order",
            "primary_key": ["PurchaseOrderID"],
            "columns": {
                "PurchaseOrderID": "STRING",
                "SubTotal": "FLOAT",
                "TotalTax": "FLOAT",
                "Total": "FLOAT",
                "TotalDiscount": "FLOAT",
                "CurrencyRate": "FLOAT",
                "SentToContact": "BOOLEAN",
                "HasAttachments": "BOOLEAN",
            },
        },
        {
            "table": "accounting_purchase_order_line_item",
            "primary_key": ["PurchaseOrderID", "LineItemID"],
            "columns": {
                "PurchaseOrderID": "STRING",
                "LineItemID": "STRING",
                "Quantity": "FLOAT",
                "UnitAmount": "FLOAT",
                "TaxAmount": "FLOAT",
                "LineAmount": "FLOAT",
                "DiscountRate": "FLOAT",
            },
        },
        {
            "table": "accounting_purchase_order_line_item_tracking",
            "primary_key": ["PurchaseOrderID", "LineItemID", "TrackingCategoryID"],
            "columns": {
                "PurchaseOrderID": "STRING",
                "LineItemID": "STRING",
                "TrackingCategoryID": "STRING",
            },
        },
        # ── Quotes ───────────────────────────────────────────────────────
        {
            "table": "accounting_quote",
            "primary_key": ["QuoteID"],
            "columns": {
                "QuoteID": "STRING",
                "SubTotal": "FLOAT",
                "TotalTax": "FLOAT",
                "Total": "FLOAT",
                "TotalDiscount": "FLOAT",
                "CurrencyRate": "FLOAT",
            },
        },
        {
            "table": "accounting_quote_line_item",
            "primary_key": ["QuoteID", "LineItemID"],
            "columns": {
                "QuoteID": "STRING",
                "LineItemID": "STRING",
                "Quantity": "FLOAT",
                "UnitAmount": "FLOAT",
                "TaxAmount": "FLOAT",
                "LineAmount": "FLOAT",
                "DiscountRate": "FLOAT",
                "DiscountAmount": "FLOAT",
            },
        },
        # ── Receipts ─────────────────────────────────────────────────────
        {
            "table": "accounting_receipt",
            "primary_key": ["ReceiptID"],
            "columns": {
                "ReceiptID": "STRING",
                "SubTotal": "FLOAT",
                "TotalTax": "FLOAT",
                "Total": "FLOAT",
                "HasAttachments": "BOOLEAN",
            },
        },
        {
            "table": "accounting_receipt_line_item",
            "primary_key": ["ReceiptID", "LineItemID"],
            "columns": {
                "ReceiptID": "STRING",
                "LineItemID": "STRING",
                "Quantity": "FLOAT",
                "UnitAmount": "FLOAT",
                "TaxAmount": "FLOAT",
                "LineAmount": "FLOAT",
            },
        },
        {
            "table": "accounting_receipt_line_item_tracking",
            "primary_key": ["ReceiptID", "LineItemID", "TrackingCategoryID"],
            "columns": {
                "ReceiptID": "STRING",
                "LineItemID": "STRING",
                "TrackingCategoryID": "STRING",
            },
        },
        # ── Repeating Invoices ───────────────────────────────────────────
        {
            "table": "accounting_repeating_invoice",
            "primary_key": ["RepeatingInvoiceID"],
            "columns": {
                "RepeatingInvoiceID": "STRING",
                "SubTotal": "FLOAT",
                "TotalTax": "FLOAT",
                "Total": "FLOAT",
                "HasAttachments": "BOOLEAN",
                "ApprovedForSending": "BOOLEAN",
                "SendCopy": "BOOLEAN",
                "MarkAsSent": "BOOLEAN",
                "IncludePDF": "BOOLEAN",
                "SchedulePeriod": "INT",
                "ScheduleDueDate": "INT",
            },
        },
        {
            "table": "accounting_repeating_invoice_line_item",
            "primary_key": ["RepeatingInvoiceID", "LineItemID"],
            "columns": {
                "RepeatingInvoiceID": "STRING",
                "LineItemID": "STRING",
                "Quantity": "FLOAT",
                "UnitAmount": "FLOAT",
                "TaxAmount": "FLOAT",
                "LineAmount": "FLOAT",
                "DiscountRate": "FLOAT",
            },
        },
        {
            "table": "accounting_repeating_invoice_line_item_tracking",
            "primary_key": ["RepeatingInvoiceID", "LineItemID", "TrackingCategoryID"],
            "columns": {
                "RepeatingInvoiceID": "STRING",
                "LineItemID": "STRING",
                "TrackingCategoryID": "STRING",
            },
        },
        # ── Tax Rates ────────────────────────────────────────────────────
        {
            "table": "accounting_tax_rate",
            "primary_key": ["TaxType"],
            "columns": {
                "TaxType": "STRING",
                "DisplayTaxRate": "FLOAT",
                "EffectiveRate": "FLOAT",
                "CanApplyToAssets": "BOOLEAN",
                "CanApplyToEquity": "BOOLEAN",
                "CanApplyToExpenses": "BOOLEAN",
                "CanApplyToLiabilities": "BOOLEAN",
                "CanApplyToRevenue": "BOOLEAN",
            },
        },
        {
            "table": "accounting_tax_rate_component",
            "primary_key": ["TaxType", "Name"],
            "columns": {
                "TaxType": "STRING",
                "Name": "STRING",
                "Rate": "FLOAT",
                "IsCompound": "BOOLEAN",
                "IsNonRecoverable": "BOOLEAN",
            },
        },
        # ── Tracking Categories ──────────────────────────────────────────
        {
            "table": "accounting_tracking_category",
            "primary_key": ["TrackingCategoryID"],
            "columns": {
                "TrackingCategoryID": "STRING",
            },
        },
        {
            "table": "accounting_tracking_option",
            "primary_key": ["TrackingCategoryID", "TrackingOptionID"],
            "columns": {
                "TrackingCategoryID": "STRING",
                "TrackingOptionID": "STRING",
            },
        },
        # ── Users ────────────────────────────────────────────────────────
        {
            "table": "accounting_user",
            "primary_key": ["UserID"],
            "columns": {
                "UserID": "STRING",
                "IsSubscriber": "BOOLEAN",
            },
        },
    ]
