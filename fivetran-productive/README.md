# Fivetran Custom Connector: Productive.io

## Why this exists

Fivetran has a [built-in Productive connector](https://fivetran.com/docs/connectors/applications/productive), but it **does not sync salary data**. This custom connector provides full feature parity with the built-in connector — same tables, same capture-deletes support — **plus salaries and 20+ additional tables** from the Productive API.

---

## Feature parity

| Feature | Built-in Fivetran | This connector |
|---------|-------------------|----------------|
| Capture deletes | 29 tables | 29 tables (same) + 21 more |
| Re-sync | Yes | Yes |
| Incremental sync | Yes | Yes |
| **Salaries** | **No** | **Yes** |
| Additional tables | — | contracts, memberships, entitlements, proposals, purchase orders, bills, overheads, and more |

---

## Quick start

### 1. Clone the repo

```bash
git clone https://github.com/autonomousminds/fivetran-os-connectors.git
cd fivetran-os-connectors/fivetran-productive
```

### 2. Set up Python

```bash
conda env create -f environment.yml
conda activate fivetran-productive
```

Or: `python3.12 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`

### 3. Configure

```bash
cp configuration.json.example configuration.json
```

Edit `configuration.json` with your credentials:
```json
{
    "api_token": "your_api_token_here",
    "organization_id": "your_organization_id_here"
}
```

Generate your API token in Productive under **Settings > API integrations**.

### 4. Test locally

```bash
python connector.py
```

Creates `files/warehouse.db` (DuckDB). Inspect with:
```bash
pip install duckdb
python -c "
import duckdb
conn = duckdb.connect('files/warehouse.db', read_only=True)
for (t,) in conn.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='tester' ORDER BY table_name\").fetchall():
    count = conn.execute(f'SELECT COUNT(*) FROM tester.\"{t}\"').fetchone()[0]
    if count > 0: print(f'{t}: {count} rows')
"
```

### 5. Deploy to Fivetran

```bash
fivetran deploy \
  --api-key YOUR_BASE64_API_KEY \
  --destination YOUR_DESTINATION_NAME \
  --connection YOUR_CONNECTION_NAME \
  --configuration configuration.json
```

Then start the initial sync from the [Fivetran Dashboard](https://fivetran.com/dashboard).

---

## Tables

The schema is **dynamic** — only tables with data in your Productive account are created in the destination. Empty endpoints are skipped.

### Reference tables (full sync every run)

Organizations, Subsidiaries, Custom Fields, Custom Field Options, Custom Field Sections, Tags, Service Types, Deal Statuses, Lost Reasons, Workflows, Workflow Statuses, Pipelines, Events, Holiday Calendars, Holidays, Document Types, Document Styles, Approval Policies, Approval Workflows, Approval Policy Assignments, Rate Cards, Tax Rates, Exchange Rates, Bank Accounts, Invoice Templates, Automatic Invoicing Rules, Payment Reminders, Payment Reminder Sequences, Deal Cost Rates, KPD Codes, Report Categories, Service Assignments, Service Type Assignments, Time Tracking Policies, Teams, Team Memberships, Sections, Folders, Organization Memberships, Integration Exporter Configurations, Integrations

### Incremental tables (cursor-based)

| Table | Filter |
|-------|--------|
| Time Entries | `after` |
| Time Entry Versions | `after` |
| Bookings | `updated_at` |
| Services | `after` |
| Activities | `after` |
| **Salaries** | **`after`** |

### Full-sync data tables

People, Users, Companies, Projects, Boards, Task Lists, Tasks, Task Dependencies, Deals, Contracts, Invoices, Invoice Attributions, Line Items, Payments, Expenses, Memberships, Comments, Attachments, Contact Entries, Pages, Page Versions, Dashboards, Filters, Prices, Entitlements, Timers, Timesheets, Overheads, Revenue Distributions, Placeholders, Purchase Orders, Bills, Todos, Discussions, Proposals, Emails, Deleted Items, Surveys, Survey Fields, Survey Field Options, Survey Responses, Resource Requests, Pulses, Widgets

---

## Architecture

```
connector.py              Entry point — dynamic schema + sync orchestration
auth.py                   API token header builder
api_client.py             Rate-limited HTTP client, JSON:API pagination, retry
helpers.py                JSON:API flattener, upsert wrapper, config validation
schema_reference.py       Reference table definitions
schema_data.py            Data table definitions
tables_reference.py       Sync logic for reference tables (41 tables)
tables_data.py            Sync logic for data tables (50 tables)
configuration.json        Credentials (gitignored)
```

---

## Rate limiting

Productive enforces 100 req/10s and 4000 req/30min. The connector uses conservative buffers (90/10s, 3800/30min) with automatic retry on 429 and exponential backoff on 5xx. If the 30-minute window is exhausted, state is checkpointed and the sync aborts for Fivetran to retry later.

---

## References

- [Productive API](https://developer.productive.io/)
- [Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk)
- [Built-in Productive Connector](https://fivetran.com/docs/connectors/applications/productive) — feature parity reference
