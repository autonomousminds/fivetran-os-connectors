"""
Sync logic for all Xero Accounting API entities.

Each sync function calls op.upsert() directly (no yield)
and mutates the state dict for cursor tracking.

All nested objects are properly flattened into columns.
All dates converted from Xero /Date()/ format to ISO 8601.
"""

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import (
    fetch_all_no_pagination,
    fetch_all_pages,
    fetch_journals,
    fetch_single,
)
from helpers import CHECKPOINT_INTERVAL, convert_xero_date, soft_delete, upsert as _upsert


def _extract_id(record, nested_key, id_field):
    """Extract an ID from a nested object: record[nested_key][id_field] → record[id_field]."""
    nested = record.pop(nested_key, None)
    if nested and isinstance(nested, dict):
        record[id_field] = nested.get(id_field, "")


def _flatten_obj(record, nested_key, prefix, fields):
    """Flatten a nested object's fields into the parent record with a prefix."""
    nested = record.pop(nested_key, None)
    if nested and isinstance(nested, dict):
        for field in fields:
            record[f"{prefix}{field}"] = nested.get(field)


def _upsert_tracking(tracking_list, tracking_table, parent_pk_fields):
    """Upsert tracking category junction records from a line item's Tracking array."""
    for tc in tracking_list:
        tc_record = dict(parent_pk_fields)
        tc_record["TrackingCategoryID"] = tc.get("TrackingCategoryID", "")
        tc_record["TrackingOptionID"] = tc.get("TrackingOptionID", "")
        tc_record["Name"] = tc.get("Name", "")
        tc_record["Option"] = tc.get("Option", "")
        _upsert(table=tracking_table, data=tc_record)


def _upsert_allocations(allocations, source_id, source_type):
    """Upsert allocation records."""
    for idx, alloc in enumerate(allocations):
        alloc_record = {
            "SourceID": source_id,
            "SourceType": source_type,
            "Index": str(idx),
            "AllocationID": alloc.get("AllocationID", ""),
            "Amount": alloc.get("Amount"),
            "Date": alloc.get("Date"),
            "IsDeleted": alloc.get("IsDeleted", False),
        }
        invoice = alloc.get("Invoice", {})
        if invoice:
            alloc_record["InvoiceID"] = invoice.get("InvoiceID", "")
            alloc_record["InvoiceNumber"] = invoice.get("InvoiceNumber", "")
        else:
            alloc_record["InvoiceID"] = ""
        credit_note = alloc.get("CreditNote", {})
        if credit_note:
            alloc_record["CreditNoteID"] = credit_note.get("CreditNoteID", "")
        _upsert(table="accounting_allocation", data=alloc_record)


def _process_line_items(line_items, parent_pk_field, parent_id, line_item_table,
                        tracking_table=None):
    """Process and upsert line items with optional tracking categories."""
    for li in line_items:
        li[parent_pk_field] = parent_id
        tracking = li.pop("Tracking", [])
        li.pop("Item", None)  # Remove nested Item object (ItemCode is the flat ref)
        li.pop("TaxBreakdown", None)
        _upsert(table=line_item_table, data=li)

        if tracking_table and tracking:
            _upsert_tracking(
                tracking, tracking_table,
                {parent_pk_field: parent_id, "LineItemID": li.get("LineItemID", "")},
            )


def _clean_record(record, remove_keys=None):
    """Remove standard nested arrays and validation fields from a record."""
    default_remove = [
        "Payments", "CreditNotes", "Prepayments", "Overpayments",
        "Attachments", "ValidationErrors", "Warnings", "StatusAttributeString",
        "HasValidationErrors", "DateString", "DueDateString",
        "InvoicePaymentServices", "InvoiceAddresses",
    ]
    for key in (remove_keys or default_remove):
        record.pop(key, None)


# ── Reference Data (full sync, no cursor) ────────────────────────────────────


def sync_organisation(config, state):
    data = fetch_single(config, "/Organisation")
    for org in data.get("Organisations", []):
        # Also upsert settings from the same API call (saves 1 call per sync)
        settings = {
            "_singleton": "settings",
            "OrganisationID": org.get("OrganisationID", ""),
            "FinancialYearEndDay": org.get("FinancialYearEndDay"),
            "FinancialYearEndMonth": org.get("FinancialYearEndMonth"),
            "SalesTaxBasis": org.get("SalesTaxBasis", ""),
            "SalesTaxPeriod": org.get("SalesTaxPeriod", ""),
            "BaseCurrency": org.get("BaseCurrency", ""),
            "CountryCode": org.get("CountryCode", ""),
            "Timezone": org.get("Timezone", ""),
            "OrganisationType": org.get("OrganisationType", ""),
        }
        _upsert(table="accounting_settings", data=settings)

        # Remove nested arrays — these are separate tables or not needed
        for key in ["Phones", "Addresses", "ExternalLinks", "PaymentTerms",
                     "ValidationErrors"]:
            org.pop(key, None)
        _upsert(table="accounting_organisation", data=org)


def sync_assets(config, state):
    from api_client import api_request
    ASSETS_BASE = "https://api.xero.com/assets.xro/1.0"
    url = f"{ASSETS_BASE}/Assets"
    page = 1
    while True:
        try:
            data = api_request(config, url, params={"page": page, "pageSize": 100, "status": "REGISTERED,DRAFT"})
        except Exception as e:
            log.warning(f"Could not fetch assets: {e}")
            return
        items = data.get("items", [])
        for asset in items:
            # Flatten nested depreciation objects
            for key in ["bookDepreciationSetting", "bookDepreciationDetail"]:
                asset.pop(key, None)
            _upsert(table="accounting_asset", data=asset)
        if len(items) < 100:
            break
        page += 1


def sync_asset_types(config, state):
    from api_client import api_request
    ASSETS_BASE = "https://api.xero.com/assets.xro/1.0"
    url = f"{ASSETS_BASE}/AssetTypes"
    try:
        data = api_request(config, url)
    except Exception as e:
        log.warning(f"Could not fetch asset types: {e}")
        return
    asset_types = data if isinstance(data, list) else data.get("assetTypes", [])
    for at in asset_types:
        at.pop("bookDepreciationSetting", None)
        _upsert(table="accounting_asset_type", data=at)


def sync_branding_themes(config, state):
    records = fetch_all_no_pagination(config, "/BrandingThemes", "BrandingThemes")
    for record in records:
        _upsert(table="accounting_branding_theme", data=record)


def sync_currencies(config, state):
    records = fetch_all_no_pagination(config, "/Currencies", "Currencies")
    for record in records:
        _upsert(table="accounting_currency", data=record)


def sync_tax_rates(config, state):
    records = fetch_all_no_pagination(config, "/TaxRates", "TaxRates")
    for record in records:
        tax_type = record.get("TaxType", "")
        components = record.pop("TaxComponents", [])
        _upsert(table="accounting_tax_rate", data=record)
        for comp in components:
            comp["TaxType"] = tax_type
            _upsert(table="accounting_tax_rate_component", data=comp)


def sync_tracking_categories(config, state):
    records = fetch_all_no_pagination(config, "/TrackingCategories", "TrackingCategories")
    for record in records:
        options = record.pop("Options", [])
        _upsert(table="accounting_tracking_category", data=record)
        for option in options:
            option["TrackingCategoryID"] = record.get("TrackingCategoryID", "")
            _upsert(table="accounting_tracking_option", data=option)


def sync_contact_groups(config, state):
    records = fetch_all_no_pagination(config, "/ContactGroups", "ContactGroups")
    for record in records:
        group_id = record.get("ContactGroupID", "")
        contacts = record.pop("Contacts", [])
        record.pop("ValidationErrors", None)
        _upsert(table="accounting_contact_group", data=record)
        for contact in contacts:
            member = {
                "ContactGroupID": group_id,
                "ContactID": contact.get("ContactID", ""),
                "Name": contact.get("Name", ""),
            }
            _upsert(table="accounting_contact_group_member", data=member)


def sync_repeating_invoices(config, state):
    records = fetch_all_no_pagination(config, "/RepeatingInvoices", "RepeatingInvoices")
    for record in records:
        record_id = record.get("RepeatingInvoiceID", "")
        line_items = record.pop("LineItems", [])

        # Flatten Contact → ContactID
        _extract_id(record, "Contact", "ContactID")

        # Flatten Schedule
        schedule = record.pop("Schedule", None)
        if schedule and isinstance(schedule, dict):
            record["SchedulePeriod"] = schedule.get("Period")
            record["ScheduleUnit"] = schedule.get("Unit")
            record["ScheduleDueDate"] = schedule.get("DueDate")
            record["ScheduleDueDateType"] = schedule.get("DueDateType")
            record["ScheduleStartDate"] = schedule.get("StartDate")
            record["ScheduleNextScheduledDate"] = schedule.get("NextScheduledDate")
            record["ScheduleEndDate"] = schedule.get("EndDate")

        _clean_record(record)
        _upsert(table="accounting_repeating_invoice", data=record)

        _process_line_items(line_items, "RepeatingInvoiceID", record_id,
                           "accounting_repeating_invoice_line_item",
                           "accounting_repeating_invoice_line_item_tracking")


# ── Incremental Entities ─────────────────────────────────────────────────────


def sync_accounts(config, state):
    cursor_key = "accounting_account_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/Accounts", "Accounts",
                                      modified_since=modified_since)
    for record in records:
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated
        record.pop("ValidationErrors", None)
        _upsert(table="accounting_account", data=record)
        soft_delete("accounting_account", {"AccountID": record.get("AccountID", "")},
                    record.get("Status", ""))

    if latest:
        state[cursor_key] = latest


def sync_contacts(config, state):
    cursor_key = "accounting_contact_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since
    count = 0

    records = fetch_all_pages(config, "/Contacts", "Contacts",
                              modified_since=modified_since)

    for record in records:
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        contact_id = record.get("ContactID", "")

        # Extract addresses into separate table
        addresses = record.pop("Addresses", [])
        for addr in addresses:
            if isinstance(addr, dict):
                addr["ContactID"] = contact_id
                _upsert(table="accounting_contact_address", data=addr)

        # Remove nested arrays we don't store inline
        for key in ["Phones", "ContactPersons", "Attachments", "ValidationErrors",
                     "ContactGroups", "SalesTrackingCategories",
                     "PurchasesTrackingCategories", "PaymentTerms", "Balances",
                     "BatchPayments", "BrandingTheme", "StatusAttributeString"]:
            record.pop(key, None)

        _upsert(table="accounting_contact", data=record)
        soft_delete("accounting_contact", {"ContactID": contact_id},
                    record.get("ContactStatus", ""))

        count += 1
        if count % CHECKPOINT_INTERVAL == 0:
            if latest:
                state[cursor_key] = latest
            op.checkpoint(state)

    if latest:
        state[cursor_key] = latest


def sync_bank_transactions(config, state):
    cursor_key = "accounting_bank_transaction_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since
    count = 0

    records = fetch_all_pages(config, "/BankTransactions", "BankTransactions",
                              modified_since=modified_since)

    for record in records:
        record_id = record.get("BankTransactionID", "")
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        line_items = record.pop("LineItems", [])

        # Flatten Contact → ContactID
        _extract_id(record, "Contact", "ContactID")

        # Flatten BankAccount → BankAccountID
        bank_account = record.pop("BankAccount", None)
        if bank_account and isinstance(bank_account, dict):
            record["BankAccountID"] = bank_account.get("AccountID", "")

        # Flatten BatchPayment
        batch_payment = record.pop("BatchPayment", None)
        if batch_payment and isinstance(batch_payment, dict):
            record["BatchPaymentID"] = batch_payment.get("BatchPaymentID", "")
            record["BatchPaymentDate"] = batch_payment.get("Date")
            record["BatchPaymentType"] = batch_payment.get("Type")
            record["BatchPaymentStatus"] = batch_payment.get("Status")
            record["BatchPaymentTotalAmount"] = batch_payment.get("TotalAmount")
            record["BatchPaymentUpdatedDateUTC"] = batch_payment.get("UpdatedDateUTC")
            record["BatchPaymentIsReconciled"] = batch_payment.get("IsReconciled")

        # Flatten ExternalLink
        ext_link = record.pop("ExternalLink", None)
        if ext_link and isinstance(ext_link, dict):
            record["ExternalLinkProviderName"] = ext_link.get("ProviderName", "")

        status = record.get("Status", "")
        _clean_record(record)
        _upsert(table="accounting_bank_transaction", data=record)
        soft_delete("accounting_bank_transaction",
                    {"BankTransactionID": record_id}, status)

        _process_line_items(line_items, "BankTransactionID", record_id,
                           "accounting_bank_transaction_line_item")

        count += 1
        if count % CHECKPOINT_INTERVAL == 0:
            if latest:
                state[cursor_key] = latest
            op.checkpoint(state)

    if latest:
        state[cursor_key] = latest


def sync_bank_transfers(config, state):
    cursor_key = "accounting_bank_transfer_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/BankTransfers", "BankTransfers",
                                      modified_since=modified_since)

    for record in records:
        updated = convert_xero_date(record.get("CreatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        # Flatten FromBankAccount and ToBankAccount
        from_acct = record.pop("FromBankAccount", None)
        if from_acct and isinstance(from_acct, dict):
            record["FromBankAccountID"] = from_acct.get("AccountID", "")

        to_acct = record.pop("ToBankAccount", None)
        if to_acct and isinstance(to_acct, dict):
            record["ToBankAccountID"] = to_acct.get("AccountID", "")

        _clean_record(record)
        _upsert(table="accounting_bank_transfer", data=record)

    if latest:
        state[cursor_key] = latest


def sync_batch_payments(config, state):
    cursor_key = "accounting_batch_payment_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/BatchPayments", "BatchPayments",
                                      modified_since=modified_since)

    for record in records:
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        # Flatten Account → AccountID
        account = record.pop("Account", None)
        if account and isinstance(account, dict):
            record["AccountID"] = account.get("AccountID", "")

        record.pop("Payments", None)
        _clean_record(record)
        _upsert(table="accounting_batch_payment", data=record)

    if latest:
        state[cursor_key] = latest


def sync_credit_notes(config, state):
    cursor_key = "accounting_credit_note_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since
    count = 0

    records = fetch_all_no_pagination(config, "/CreditNotes", "CreditNotes",
                                      modified_since=modified_since)

    for record in records:
        record_id = record.get("CreditNoteID", "")
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        line_items = record.pop("LineItems", [])
        allocations = record.pop("Allocations", [])
        status = record.get("Status", "")
        _extract_id(record, "Contact", "ContactID")
        _clean_record(record)
        _upsert(table="accounting_credit_note", data=record)
        soft_delete("accounting_credit_note",
                    {"CreditNoteID": record_id}, status)

        _process_line_items(line_items, "CreditNoteID", record_id,
                           "accounting_credit_note_line_item",
                           "accounting_credit_note_line_item_tracking")

        if allocations:
            _upsert_allocations(allocations, record_id, "CreditNote")

        count += 1
        if count % CHECKPOINT_INTERVAL == 0:
            if latest:
                state[cursor_key] = latest
            op.checkpoint(state)

    if latest:
        state[cursor_key] = latest


def sync_employees(config, state):
    cursor_key = "accounting_employee_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/Employees", "Employees",
                                      modified_since=modified_since)
    for record in records:
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        # Flatten ExternalLink
        ext = record.pop("ExternalLink", None)
        record.pop("ValidationErrors", None)
        _upsert(table="accounting_employee", data=record)

    if latest:
        state[cursor_key] = latest


def sync_expense_claims(config, state):
    cursor_key = "accounting_expense_claim_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/ExpenseClaims", "ExpenseClaims",
                                      modified_since=modified_since)

    for record in records:
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        # Flatten User → UserID
        user = record.pop("User", None)
        if user and isinstance(user, dict):
            record["UserID"] = user.get("UserID", "")

        record.pop("Receipts", None)
        record.pop("Payments", None)
        _clean_record(record)
        _upsert(table="accounting_expense_claim", data=record)

    if latest:
        state[cursor_key] = latest


def sync_invoices(config, state):
    cursor_key = "accounting_invoice_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since
    count = 0

    records = fetch_all_pages(config, "/Invoices", "Invoices",
                              modified_since=modified_since)

    for record in records:
        record_id = record.get("InvoiceID", "")
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        line_items = record.pop("LineItems", [])
        status = record.get("Status", "")
        _extract_id(record, "Contact", "ContactID")
        _clean_record(record)
        _upsert(table="accounting_invoice", data=record)
        soft_delete("accounting_invoice", {"InvoiceID": record_id}, status)

        _process_line_items(line_items, "InvoiceID", record_id,
                           "accounting_invoice_line_item",
                           "accounting_invoice_line_item_tracking")

        count += 1
        if count % CHECKPOINT_INTERVAL == 0:
            if latest:
                state[cursor_key] = latest
            op.checkpoint(state)

    if latest:
        state[cursor_key] = latest


def sync_items(config, state):
    cursor_key = "accounting_item_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/Items", "Items",
                                      modified_since=modified_since)

    for record in records:
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        # Flatten PurchaseDetails
        pd = record.pop("PurchaseDetails", None)
        if pd and isinstance(pd, dict):
            record["PurchaseUnitPrice"] = pd.get("UnitPrice")
            record["PurchaseAccountCode"] = pd.get("AccountCode")
            record["PurchaseTaxType"] = pd.get("TaxType")

        # Flatten SalesDetails
        sd = record.pop("SalesDetails", None)
        if sd and isinstance(sd, dict):
            record["SalesUnitPrice"] = sd.get("UnitPrice")
            record["SalesAccountCode"] = sd.get("AccountCode")
            record["SalesTaxType"] = sd.get("TaxType")

        _clean_record(record)
        _upsert(table="accounting_item", data=record)

    if latest:
        state[cursor_key] = latest


def sync_journals(config, state):
    cursor_key = "accounting_journal_offset"
    offset = int(state.get(cursor_key, 0))

    for page, new_offset in fetch_journals(config, offset=offset):
        for journal in page:
            journal_id = journal.get("JournalID", "")
            journal_lines = journal.pop("JournalLines", [])

            _upsert(table="accounting_journal", data=journal)

            for idx, line in enumerate(journal_lines):
                line["JournalID"] = journal_id
                if "JournalLineID" not in line:
                    line["JournalLineID"] = str(idx)
                journal_line_id = line.get("JournalLineID", str(idx))

                tracking = line.pop("TrackingCategories", [])
                _upsert(table="accounting_journal_line", data=line)

                for tc in tracking:
                    tc_record = {
                        "JournalID": journal_id,
                        "JournalLineID": journal_line_id,
                        "TrackingCategoryID": tc.get("TrackingCategoryID", ""),
                        "TrackingOptionID": tc.get("TrackingOptionID", ""),
                        "Name": tc.get("Name", ""),
                        "Option": tc.get("Option", ""),
                    }
                    _upsert(table="accounting_journal_line_tracking", data=tc_record)

        state[cursor_key] = str(new_offset)
        op.checkpoint(state)


def sync_linked_transactions(config, state):
    cursor_key = "accounting_linked_transaction_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/LinkedTransactions", "LinkedTransactions",
                                      modified_since=modified_since)
    for record in records:
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated
        _clean_record(record)
        _upsert(table="accounting_linked_transaction", data=record)

    if latest:
        state[cursor_key] = latest


def sync_manual_journals(config, state):
    cursor_key = "accounting_manual_journal_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_pages(config, "/ManualJournals", "ManualJournals",
                              modified_since=modified_since)

    for record in records:
        record_id = record.get("ManualJournalID", "")
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        # Extract JournalLines into child table
        journal_lines = record.pop("JournalLines", [])
        status = record.get("Status", "")
        _clean_record(record)
        _upsert(table="accounting_manual_journal", data=record)
        soft_delete("accounting_manual_journal",
                    {"ManualJournalID": record_id}, status)

        for idx, line in enumerate(journal_lines):
            line["ManualJournalID"] = record_id
            # ManualJournalLines don't have LineItemID — generate one
            if "LineItemID" not in line:
                line["LineItemID"] = str(idx)
            line.pop("Tracking", None)
            _upsert(table="accounting_manual_journal_line", data=line)

    if latest:
        state[cursor_key] = latest


def sync_overpayments(config, state):
    cursor_key = "accounting_overpayment_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/Overpayments", "Overpayments",
                                      modified_since=modified_since)

    for record in records:
        record_id = record.get("OverpaymentID", "")
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        line_items = record.pop("LineItems", [])
        allocations = record.pop("Allocations", [])
        _extract_id(record, "Contact", "ContactID")
        _clean_record(record)
        _upsert(table="accounting_overpayment", data=record)

        _process_line_items(line_items, "OverpaymentID", record_id,
                           "accounting_overpayment_line_item")

        if allocations:
            _upsert_allocations(allocations, record_id, "Overpayment")

    if latest:
        state[cursor_key] = latest


def sync_payments(config, state):
    cursor_key = "accounting_payment_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since
    count = 0

    records = fetch_all_no_pagination(config, "/Payments", "Payments",
                              modified_since=modified_since)

    for record in records:
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        # Flatten Invoice → InvoiceID, InvoiceNumber
        invoice = record.pop("Invoice", None)
        if invoice and isinstance(invoice, dict):
            record["InvoiceID"] = invoice.get("InvoiceID", "")
            record["InvoiceNumber"] = invoice.get("InvoiceNumber", "")

        # Flatten CreditNote → CreditNoteID, CreditNoteNumber
        cn = record.pop("CreditNote", None)
        if cn and isinstance(cn, dict):
            record["CreditNoteID"] = cn.get("CreditNoteID", "")
            record["CreditNoteNumber"] = cn.get("CreditNoteNumber", "")

        # Flatten Prepayment → PrepaymentID
        pp = record.pop("Prepayment", None)
        if pp and isinstance(pp, dict):
            record["PrepaymentID"] = pp.get("PrepaymentID", "")

        # Flatten Overpayment → OverpaymentID
        ovp = record.pop("Overpayment", None)
        if ovp and isinstance(ovp, dict):
            record["OverpaymentID"] = ovp.get("OverpaymentID", "")

        # Flatten Account → AccountID, Code
        acct = record.pop("Account", None)
        if acct and isinstance(acct, dict):
            record["AccountID"] = acct.get("AccountID", "")
            record["Code"] = acct.get("Code", "")

        # Flatten BatchPayment → BatchPaymentID
        bp = record.pop("BatchPayment", None)
        if bp and isinstance(bp, dict):
            record["BatchPaymentID"] = bp.get("BatchPaymentID", "")

        status = record.get("Status", "")
        _clean_record(record)
        _upsert(table="accounting_payment", data=record)
        soft_delete("accounting_payment", {"PaymentID": record.get("PaymentID", "")},
                    status)

        count += 1
        if count % CHECKPOINT_INTERVAL == 0:
            if latest:
                state[cursor_key] = latest
            op.checkpoint(state)

    if latest:
        state[cursor_key] = latest


def sync_prepayments(config, state):
    cursor_key = "accounting_prepayment_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/Prepayments", "Prepayments",
                                      modified_since=modified_since)

    for record in records:
        record_id = record.get("PrepaymentID", "")
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        line_items = record.pop("LineItems", [])
        allocations = record.pop("Allocations", [])
        _extract_id(record, "Contact", "ContactID")
        _clean_record(record)
        _upsert(table="accounting_prepayment", data=record)

        _process_line_items(line_items, "PrepaymentID", record_id,
                           "accounting_prepayment_line_item")

        if allocations:
            _upsert_allocations(allocations, record_id, "Prepayment")

    if latest:
        state[cursor_key] = latest


def sync_purchase_orders(config, state):
    cursor_key = "accounting_purchase_order_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since
    count = 0

    records = fetch_all_no_pagination(config, "/PurchaseOrders", "PurchaseOrders",
                                      modified_since=modified_since)

    for record in records:
        record_id = record.get("PurchaseOrderID", "")
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        line_items = record.pop("LineItems", [])
        status = record.get("Status", "")
        _extract_id(record, "Contact", "ContactID")
        _clean_record(record)
        _upsert(table="accounting_purchase_order", data=record)
        soft_delete("accounting_purchase_order",
                    {"PurchaseOrderID": record_id}, status)

        _process_line_items(line_items, "PurchaseOrderID", record_id,
                           "accounting_purchase_order_line_item",
                           "accounting_purchase_order_line_item_tracking")

        count += 1
        if count % CHECKPOINT_INTERVAL == 0:
            if latest:
                state[cursor_key] = latest
            op.checkpoint(state)

    if latest:
        state[cursor_key] = latest


def sync_quotes(config, state):
    cursor_key = "accounting_quote_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/Quotes", "Quotes",
                                      modified_since=modified_since)

    for record in records:
        record_id = record.get("QuoteID", "")
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        line_items = record.pop("LineItems", [])
        status = record.get("Status", "")
        _extract_id(record, "Contact", "ContactID")
        _clean_record(record)
        _upsert(table="accounting_quote", data=record)
        soft_delete("accounting_quote", {"QuoteID": record_id}, status)

        _process_line_items(line_items, "QuoteID", record_id,
                           "accounting_quote_line_item")

    if latest:
        state[cursor_key] = latest


def sync_receipts(config, state):
    cursor_key = "accounting_receipt_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/Receipts", "Receipts",
                                      modified_since=modified_since)

    for record in records:
        receipt_id = record.get("ReceiptID", "")
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated

        line_items = record.pop("LineItems", [])

        # Flatten Contact → ContactID
        _extract_id(record, "Contact", "ContactID")

        # Flatten User → UserID
        user = record.pop("User", None)
        if user and isinstance(user, dict):
            record["UserID"] = user.get("UserID", "")

        _clean_record(record)
        _upsert(table="accounting_receipt", data=record)

        _process_line_items(line_items, "ReceiptID", receipt_id,
                           "accounting_receipt_line_item",
                           "accounting_receipt_line_item_tracking")

    if latest:
        state[cursor_key] = latest


def sync_users(config, state):
    cursor_key = "accounting_user_modified_since"
    modified_since = state.get(cursor_key)
    latest = modified_since

    records = fetch_all_no_pagination(config, "/Users", "Users",
                                      modified_since=modified_since)
    for record in records:
        updated = convert_xero_date(record.get("UpdatedDateUTC", ""))
        if updated and (not latest or updated > latest):
            latest = updated
        record.pop("ValidationErrors", None)
        _upsert(table="accounting_user", data=record)

    if latest:
        state[cursor_key] = latest


# ── Sync order: reference tables first, then incremental ─────────────────────

ACCOUNTING_REFERENCE_SYNCS = [
    sync_organisation,  # also syncs accounting_settings in the same API call
    sync_assets,
    sync_asset_types,
    sync_branding_themes,
    sync_currencies,
    sync_tax_rates,
    sync_tracking_categories,
    sync_contact_groups,
    sync_repeating_invoices,
]

ACCOUNTING_INCREMENTAL_SYNCS = [
    sync_accounts,
    sync_contacts,
    sync_bank_transactions,
    sync_bank_transfers,
    sync_batch_payments,
    sync_credit_notes,
    sync_employees,
    sync_expense_claims,
    sync_invoices,
    sync_items,
    sync_journals,
    sync_linked_transactions,
    sync_manual_journals,
    sync_overpayments,
    sync_payments,
    sync_prepayments,
    sync_purchase_orders,
    sync_quotes,
    sync_receipts,
    sync_users,
]
