# Zoho Creator — Fivetran Custom Connector

Syncs **every application, form, report and record** the OAuth grant can see
out of [Zoho Creator](https://www.zoho.com/creator/) into a Fivetran
destination. Discovery is dynamic: the connector calls Zoho's `/meta`
endpoints at the start of every run, builds the table list from what it
finds, and continues to sync new forms automatically as they're added.

Built against **Zoho Creator API v2.1**. Documentation:
<https://www.zoho.com/creator/help/api/v2.1/>.

## Tables

### Fixed meta tables

| Table             | Primary key                                            |
|-------------------|--------------------------------------------------------|
| `applications`    | `link_name`                                            |
| `forms`           | `app_link_name`, `link_name`                           |
| `reports`         | `app_link_name`, `link_name`                           |
| `form_fields`     | `app_link_name`, `form_link_name`, `link_name`         |
| `subform_fields`  | `app_link_name`, `form_link_name`, `link_name`         |

### Dynamic data tables

For every form discovered, one table:

```
data_{app_link_name}__{form_link_name}              (primary key: ID)
```

Plus one extra child table per subform field on that form:

```
data_{app_link_name}__{form_link_name}__sub_{subform_field}   (primary key: ID, FK: parent_id)
```

Column lists are inferred by Fivetran from the first upsert — adding a new
field in Zoho Creator just shows up as a new column on the next sync.

## Sync strategy

- **Default — full syncs use the Data API** with `field_config=all` and
  cursor pagination (200-row pages). This returns every native form field,
  every lookup, and every computed/aggregate column.
- **Subsequent runs**: same Data API endpoint filtered by
  `Modified_Time > '{last_run}'`.
- **Every 7 days per form**: a full re-sync runs to catch records
  hard-deleted in Zoho (the Modified_Time filter never returns deleted IDs).
  Missing IDs are emitted as `op.delete`.

### Bulk Read (opt-in, fast but lean)

Set `"prefer_bulk_read": true` in `configuration.json` to enable Zoho's
async Bulk Read API for full syncs. **Trade-off:** Bulk Read is up to
~365× faster on tables with 50k+ rows (one job vs hundreds of paginated
GETs), but it returns only the columns visible in the report VIEW — not
the full record from the underlying form. For most analytical use cases
you want the Data API's complete column set.

Bulk Read requires two OAuth scopes:
`ZohoCreator.bulk.CREATE` (job creation) and
`ZohoCreator.bulk.READ` (status polling + result download).

If Bulk Read fails for a specific report (chart/pivot reports return
code 7150 "not supported"; some Category-2 shared apps reject the poll
endpoint), we fall back to the Data API for that report only.

## OAuth Self-Client setup (one-time)

The connector consumes a long-lived `refresh_token` from configuration.json.
You generate it once with Zoho's Self-Client flow:

1. Visit `https://api-console.zoho.{dc}` for your data center
   (`eu`, `com`, `in`, `com.au`, `com.cn`, `jp`, `cloud.ca`).
2. Create a new **Self Client** (not Server-based, not Mobile). Copy the
   `Client ID` and `Client Secret`.
3. Under the Self Client → **Generate Code** tab, paste these scopes
   (space-separated):

   ```
   ZohoCreator.dashboard.READ ZohoCreator.meta.application.READ ZohoCreator.meta.form.READ ZohoCreator.report.READ
   ```

   Choose a long duration (Zoho caps at 10 minutes for the **grant code**
   — that's fine, we only need it once). Click **Create**, copy the code.
4. Exchange the grant code for a `refresh_token` via curl (substitute your
   data-center accounts host):

   ```bash
   curl -X POST 'https://accounts.zoho.eu/oauth/v2/token' \
     -d 'grant_type=authorization_code' \
     -d 'client_id=YOUR_CLIENT_ID' \
     -d 'client_secret=YOUR_CLIENT_SECRET' \
     -d 'code=YOUR_GRANT_CODE'
   ```

   The response carries `"refresh_token": "1000.xxx..."`. **Store it in a
   password manager** — Zoho only shows it once, and regenerating it
   silently invalidates the previous one if you exceed 20 concurrent
   tokens on the same client.
5. Fill in `configuration.json` (copy from `configuration.json.example`).

## Configuration

```json
{
    "client_id":          "1000.XXXXXXXXXXXXXXXXXX",
    "client_secret":      "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "refresh_token":      "1000.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.xxxxxxxxxxxx",
    "account_owner_name": "your_zoho_login_name",
    "data_center":        "eu"
}
```

- `data_center` ∈ `{com, eu, in, com.au, com.cn, jp, ca}`.
- `account_owner_name` is the Zoho login name used in URL paths for
  Category-1 apps. For Category-2 apps (shared from another workspace) the
  owner is auto-discovered from `/meta/applications`.

## Running locally

```bash
conda activate fivetran-productive    # env already has fivetran-connector-sdk, duckdb, requests
cd zoho_creator
python validate.py                    # Apple-Silicon-safe local sync; writes files/local_warehouse.db
```

The standard SDK debugger also works on Linux / Intel macOS, but **crashes
on Apple Silicon** (x86_64 JVM under Rosetta + DuckDB JNI). Use `validate.py`
locally on M-series Macs:

```bash
fivetran debug --configuration configuration.json   # NOT on Apple Silicon
fivetran deploy --api-key <KEY> --destination <DEST> --connection zoho_creator --configuration configuration.json
```

Inspect local output:

```bash
duckdb files/local_warehouse.db -c "SHOW TABLES;"
duckdb files/local_warehouse.db -c "SELECT app_link_name, link_name FROM forms;"
duckdb files/local_warehouse.db -c "SELECT count(*) FROM data_myapp__customers;"
```

## Caveats (handled in the connector, but worth knowing)

1. **Records read through reports, not forms.** Each form normally has an
   auto-created "All_<Form>" list report. If a form doesn't have any
   list-style report we skip its records (meta still synced) and log a
   warning. Fix by enabling a default report in Zoho Creator.
2. **Hard deletes** are invisible to the Modified_Time filter. We schedule
   a full Bulk Read re-sync every 7 days per form to catch them via diff.
3. **Rate limits.** Zoho documents 50 req/min per endpoint per IP. Our
   global limiter caps at 45/min across all endpoints, which is conservative
   but safe when many forms are syncing concurrently from one IP.
4. **Daily quota** varies by Zoho plan. On 429 with `Retry-After` > 300s we
   treat it as a daily exhaustion: checkpoint and exit. Fivetran retries on
   the next scheduled run.
5. **Bulk Read 200k-row cap.** Forms larger than that today are not
   automatically chunked in v1 — they will sync the first 200k rows on
   bulk runs and rely entirely on the Modified_Time incremental for new
   data. If you have a single form >200k rows, open an issue.
6. **OAuth refresh-token revocation.** Zoho keeps roughly 20 refresh
   tokens per client_id; the 21st silently invalidates the oldest.
   Generating a fresh refresh_token in api-console.zoho.{dc} and re-deploying
   the config fixes 401 errors.
7. **Multi-DC token mismatch.** Tokens generated against
   `accounts.zoho.com` will not work against `zohoapis.eu`. The connector
   uses whichever `data_center` is in config; make sure your Self Client
   was created in the same data center.
8. **Subforms.** A subform's rows are extracted into a separate child
   table with a `parent_id` FK. Multi-select fields are JSON-encoded
   inline rather than exploded into junction tables (the resulting column
   is still queryable in modern warehouses).
9. **File uploads.** We store the file's URL and filename but do NOT
   download the binary content.
10. **`field_config=all`** is hard-coded so we get every field, including
    formula/computed fields. (Default `quick_view` omits roughly half.)
11. **`environment`** header defaults to `production`. Syncing `stage` or
    `development` environments is not exposed in v1.

## File layout

```
zoho_creator/
├── README.md                  this file
├── requirements.txt           fivetran-connector-sdk, requests, duckdb
├── configuration.json.example fill in and copy to configuration.json
├── connector.py               schema() + update() entry points
├── auth.py                    OAuth refresh-token cache + data-center routing
├── api_client.py              rate limiter, Data API cursor pagination, Bulk Read job
├── helpers.py                 STATE_VERSION, flatten/extract, hard-delete reconciliation
├── schema.py                  dynamic schema discovery (cached)
├── tables_meta.py             upsert meta tables (applications/forms/reports/...)
├── tables_data.py             record sync per form (bulk + incremental)
└── validate.py                Apple-Silicon local-test harness (DuckDB native)
```
