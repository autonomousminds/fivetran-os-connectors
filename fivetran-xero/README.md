# Fivetran Custom Connector: Xero Accounting + UK Payroll

Custom Fivetran connector that syncs **all Xero Accounting API data** plus **all UK Payroll API data** — filling the gap left by Fivetran's built-in Xero connector which only supports Australian payroll.

## What's Included

- **Full parity** with the built-in Fivetran Xero connector (52 accounting tables)
- **29 UK Payroll tables** including employees, pay runs, payslips, tax, leave, salary, deductions, benefits, and more
- **81 total tables** across Accounting, Assets, and UK Payroll APIs
- Incremental sync via `If-Modified-Since` for accounting entities
- Automatic date conversion from Xero's `/Date(ms)/` format to ISO 8601
- Rate limiting (55 req/min sliding window with automatic retry on 429)
- Per-employee checkpointing for payroll resumability
- Nested objects properly flattened (Contact → ContactID, BankAccount → BankAccountID, etc.)

---

## Quick Start

### Step 1: Clone the repo

```bash
git clone https://github.com/autonomousminds/fivetran-os-connectors.git
cd fivetran-os-connectors/fivetran-xero
```

### Step 2: Set up Python environment

Using conda (recommended):
```bash
conda create -n fivetran-xero python=3.12 -y
conda activate fivetran-xero
pip install fivetran-connector-sdk requests
```

Or using pip/venv:
```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install fivetran-connector-sdk requests
```

### Step 3: Create a Xero Custom Connection App

1. Go to the [Xero Developer Portal](https://developer.xero.com/app/manage)
2. Click **New App** and select **Custom Connection**
   > Custom Connections are available for organisations in AU, NZ, UK, or US
3. Give your app a name (e.g., "Fivetran Sync")
4. Select the following **scopes**:
   - `accounting.transactions.read`
   - `accounting.settings.read`
   - `accounting.contacts.read`
   - `accounting.journals.read`
   - `accounting.attachments.read`
   - `assets.read`
   - `payroll.employees.read`
   - `payroll.settings.read`
   - `payroll.timesheets.read`
   - `payroll.payruns.read`
   - `payroll.payslip.read`
5. Click **Create App**
6. Authorize the app to connect to your Xero organisation
7. Copy your **Client ID** and **Client Secret**

### Step 4: Configure credentials

```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```env
XERO_CLIENT_ID=your_client_id_here
XERO_CLIENT_SECRET=your_client_secret_here
```

> The tenant ID is automatically resolved — Custom Connection apps are bound to a single organisation.

### Step 5: Test locally

```bash
python connector.py
```

This runs the connector in debug mode and creates `files/warehouse.db` (a DuckDB database) with all synced data.

To inspect the results:
```bash
pip install duckdb
python -c "
import duckdb
conn = duckdb.connect('files/warehouse.db', read_only=True)
tables = conn.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='tester' ORDER BY table_name\").fetchall()
for (t,) in tables:
    count = conn.execute(f'SELECT COUNT(*) FROM tester.\"{t}\"').fetchone()[0]
    if count > 0:
        print(f'{t}: {count} rows')
"
```

To reset state and do a full re-sync:
```bash
rm -rf files/
python connector.py
```

### Step 6: Deploy to Fivetran

1. Get your Fivetran API key from **Settings > API Key** in the [Fivetran Dashboard](https://fivetran.com/dashboard)
2. Base64-encode it: `echo -n "key:secret" | base64`
3. Create `configuration.json` with your Xero credentials:
   ```json
   {
       "client_id": "your_actual_client_id",
       "client_secret": "your_actual_client_secret"
   }
   ```
4. Deploy:
   ```bash
   fivetran deploy \
     --api-key YOUR_BASE64_API_KEY \
     --destination YOUR_DESTINATION_NAME \
     --connection YOUR_CONNECTION_NAME \
     --configuration configuration.json
   ```
5. Go to the Fivetran Dashboard and start the initial sync

> To redeploy after code changes, run the same `fivetran deploy` command with the same connection name.

---

## Tables Synced

### Accounting API (52 tables)

| Category | Tables |
|----------|--------|
| **Core** | Organisation, Settings, Accounts, Contacts, Contact Addresses, Contact Groups, Contact Group Members |
| **Transactions** | Invoices, Credit Notes, Bank Transactions, Payments, Receipts, Purchase Orders, Quotes, Overpayments, Prepayments, Expense Claims, Linked Transactions |
| **Line Items** | Invoice, Credit Note, Bank Transaction, Receipt, Purchase Order, Quote, Overpayment, Prepayment, Repeating Invoice Line Items, Manual Journal Lines, Journal Lines |
| **Tracking** | Tracking Categories, Tracking Options, + 6 junction tables (Invoice/Credit Note/Journal/Receipt/Purchase Order/Repeating Invoice line item tracking) |
| **Other** | Journals, Manual Journals, Allocations, Items, Tax Rates, Tax Rate Components, Currencies, Branding Themes, Bank Transfers, Batch Payments, Users, Repeating Invoices, Assets, Asset Types |

### UK Payroll API (29 tables)

| Category | Tables |
|----------|--------|
| **Employees** | Employees, Employment, Tax, Opening Balances, Leave, Leave Balances, Statutory Leave Balances, Pay Templates, Salary & Wages, Payment Methods |
| **Pay Runs** | Pay Runs, Payslips + 8 line types (Earnings, Deductions, Leave Accrual, Reimbursement, Benefit, Tax, Court Order, Payment) |
| **Reference** | Leave Types, Earning Rates, Deductions, Benefits, Reimbursements, Earnings Orders, Timesheets, Timesheet Lines, Settings |

---

## Sync Strategy

| Data Type | Strategy |
|-----------|----------|
| Reference tables (currencies, tax rates, etc.) | Full sync every run |
| Accounting entities (invoices, contacts, etc.) | Incremental via `If-Modified-Since` header |
| Journals | Offset-based incremental |
| Contacts & addresses | Full sync every run (matches built-in connector) |
| UK Payroll | Full sync every run (API doesn't support `If-Modified-Since`) |

---

## Architecture

```
connector.py            → Entry point (schema + update)
auth.py                 → OAuth2 client_credentials token management
api_client.py           → Rate-limited HTTP client with pagination & retries
schema_accounting.py    → 52 accounting table definitions (from Xero OpenAPI spec)
schema_payroll.py       → 29 payroll table definitions (from Xero OpenAPI spec)
tables_accounting.py    → Sync logic for all accounting entities
tables_payroll.py       → Sync logic for all payroll entities
.env                    → Local credentials (gitignored)
.env.example            → Template for .env
configuration.json      → Deployment credentials template (gitignored)
```

---

## Rate Limiting

The connector respects Xero's 60 calls/minute limit with a sliding window rate limiter (55 calls/min buffer). Automatic retry on 429 responses with `Retry-After` header support. Exponential backoff on 5xx errors.

---

## API References

### Xero
- [Accounting API Overview](https://developer.xero.com/documentation/api/accounting/overview)
- [UK Payroll API Overview](https://developer.xero.com/documentation/api/payrolluk/overview)
- [Xero OpenAPI Specs](https://github.com/XeroAPI/Xero-OpenAPI) — authoritative source for all field definitions
  - [xero_accounting.yaml](https://raw.githubusercontent.com/XeroAPI/Xero-OpenAPI/master/xero_accounting.yaml)
  - [xero-payroll-uk.yaml](https://raw.githubusercontent.com/XeroAPI/Xero-OpenAPI/master/xero-payroll-uk.yaml)

### Fivetran
- [Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide)
- [Connector SDK Technical Reference](https://fivetran.com/docs/connector-sdk/technical-reference)
- [Connector SDK Best Practices](https://fivetran.com/docs/connector-sdk/best-practices)
- [Connector SDK GitHub](https://github.com/fivetran/fivetran_connector_sdk)
- [Built-in Fivetran Xero Connector](https://fivetran.com/docs/connectors/applications/xero) — feature parity reference
