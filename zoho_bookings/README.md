# Zoho Bookings — Fivetran Custom Connector

Syncs **workspaces, services, staff, resources, and appointments** out of
[Zoho Bookings](https://www.zoho.com/bookings/) into a Fivetran destination.
Built against **Zoho Bookings API v1**. Documentation:
<https://www.zoho.com/bookings/help/api/v1/oauthauthentication.html>.

Modelled after the sister `zoho_creator` connector and shares its `auth.py`
pattern, rate limiter, and Apple-Silicon validation harness.

## Tables

| Table                              | Primary key                  | Source endpoint                                |
|------------------------------------|------------------------------|------------------------------------------------|
| `workspaces`                       | `id`                         | `GET /bookings/v1/json/workspaces`             |
| `services`                         | `id`                         | `GET /bookings/v1/json/services` (per ws)      |
| `staff`                            | `id`                         | `GET /bookings/v1/json/staffs` (per ws)        |
| `resources`                        | `id`                         | `GET /bookings/v1/json/resources`              |
| `appointments`                     | `booking_id`                 | `POST /bookings/v1/json/fetchappointment`      |
| `service_staff_assignments`        | `service_id`, `staff_id`     | derived from `services.assigned_staffs`        |
| `service_workspace_assignments`    | `service_id`, `workspace_id` | derived from `services.assigned_workspace`     |
| `staff_service_assignments`        | `staff_id`, `service_id`     | derived from `staff.assigned_services`         |
| `staff_workspace_assignments`      | `staff_id`, `workspace_id`   | derived from `staff.assigned_workspaces`       |

The four bridge tables let you join service ↔ staff and service ↔ workspace
cleanly in SQL — the underlying JSON arrays are dropped from the parent rows.

## Sync strategy

**Pure full re-sync every run.** Each invocation fetches the complete current
state for every table. Simple, always reflects status changes (cancellations,
reschedules, completions) — no incremental cursor bookkeeping.

- Meta tables (workspaces / services / staff / resources): one or two `GET`s
  per workspace; tiny responses.
- Appointments: paginated POST to `/fetchappointment` with a date window
  `[today - past_window_days, today + future_window_days]` (default 365 / 365).
  Pagination is `page=1..N` with `per_page=100` (60 if you have custom fields
  enabled — see config), looping until `next_page_available=false`.
- Hard-delete reconciliation: each table records all IDs seen this run,
  diffs against the previous run's snapshot in state, and emits `op.delete`
  for missing IDs.

## OAuth Self-Client setup (one-time)

The connector consumes a long-lived `refresh_token` from `configuration.json`.
Generate it once with Zoho's Self-Client flow:

1. Visit `https://api-console.zoho.{dc}` for your data center (`eu`, `com`,
   `in`, `com.au`, `com.cn`, `jp`, `sa`, `cloud.ca`).
2. Create a new **Self Client**. Copy the `Client ID` and `Client Secret`.
3. Under the Self Client → **Generate Code** tab, paste the scope:

   ```
   zohobookings.data.CREATE
   ```

   (That single scope is the only one documented for Zoho Bookings; it
   covers both read and write endpoints.) Click **Create**, copy the code.
4. Exchange the grant code for a `refresh_token`:

   ```bash
   curl -X POST 'https://accounts.zoho.eu/oauth/v2/token' \
     -d 'grant_type=authorization_code' \
     -d 'client_id=YOUR_CLIENT_ID' \
     -d 'client_secret=YOUR_CLIENT_SECRET' \
     -d 'code=YOUR_GRANT_CODE'
   ```

   The response carries `"refresh_token": "1000.xxx..."`. **Store it in a
   password manager** — Zoho only shows it once.
5. Fill in `configuration.json` (copy from `configuration.json.example`).

## Configuration

```json
{
    "client_id":                      "1000.XXXXXXXXXXXXXXXXXX",
    "client_secret":                  "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "refresh_token":                  "1000.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.xxxxxxxxxxxx",
    "data_center":                    "eu",
    "workspaces":                     "",
    "appointments_past_window_days":  "365",
    "appointments_future_window_days":"365",
    "appointments_per_page":          "100",
    "bookings_has_custom_fields":     "false"
}
```

- `data_center` ∈ `{com, eu, in, com.au, com.cn, jp, sa, ca}`.
- `workspaces` — optional **comma-separated string** of workspace IDs to
  limit syncing to (e.g. `"3848021000000027004,3848021000000027005"`).
  Leave empty (`""`) to sync every workspace the token can see. Fivetran
  requires all config values to be strings, so this isn't a JSON array.
- `appointments_past_window_days` / `appointments_future_window_days` —
  how far back / forward to fetch appointments each run. Default 365 each.
- `appointments_per_page` — pagination size. Max 100 normally, 60 if
  `bookings_has_custom_fields` is `"true"`.
- `bookings_has_custom_fields` — set to `"true"` if you've enabled per-form
  custom fields in Bookings. This caps page size at 60 (Zoho's limit).

## Running locally

```bash
conda activate fivetran-productive   # already has fivetran-connector-sdk, duckdb, requests
cd zoho_bookings
cp configuration.json.example configuration.json
# edit configuration.json with real credentials
python validate.py                   # Apple-Silicon-safe; writes files/local_warehouse.db
```

The standard SDK debugger works on Linux / Intel macOS but **crashes on
Apple Silicon** (x86_64 JVM under Rosetta + DuckDB JNI). Use `validate.py`
locally on M-series Macs:

```bash
fivetran debug --configuration configuration.json   # NOT on Apple Silicon
fivetran deploy --api-key <KEY> --destination <DEST> --connection zoho_bookings --configuration configuration.json
```

Inspect local output:

```bash
duckdb files/local_warehouse.db -c "SHOW TABLES;"
duckdb files/local_warehouse.db -c "SELECT COUNT(*) FROM appointments;"
duckdb files/local_warehouse.db -c "SELECT booking_id, customer_name, service_name, start_time, status FROM appointments LIMIT 10;"
duckdb files/local_warehouse.db -c "SELECT s.name AS service, st.name AS staff FROM services s JOIN service_staff_assignments ssa ON s.id = ssa.service_id JOIN staff st ON st.id = ssa.staff_id LIMIT 10;"
```

## Caveats (handled in the connector, but worth knowing)

1. **Daily API quota.** Zoho Bookings caps total API calls at 250–3000 per
   user per day depending on plan tier. We treat `429` with `Retry-After`
   greater than 300s as a daily-quota exhaustion: checkpoint and exit so
   Fivetran retries on the next scheduled run.
2. **Per-minute rate limit.** Zoho doesn't publish one for Bookings, but
   the same infra throttles bursts. The connector keeps a conservative
   45/min per-endpoint + 250/min global ceiling reused from the Creator
   client. You should never hit it under normal sync sizes.
3. **Full re-sync every run.** No incremental cursor — every run pulls
   the entire date window. This is what catches status changes
   (cancellations, reschedules). If you have very high appointment volumes
   and bump up against the daily quota, shrink the date window in config.
4. **Hard deletes** are reconciled by diffing the current ID set against
   the previous run's snapshot in state. Any missing IDs are emitted as
   `op.delete`.
5. **Custom fields** on appointments are returned in a nested
   `customer_more_info` dict; the connector flattens them inline with the
   prefix `customer_more_info_`. New custom fields appear automatically as
   new columns on the next sync.
6. **Pagination cap.** `per_page` is 100 normally, but Zoho caps at 60
   when custom fields are enabled. Toggle `bookings_has_custom_fields`
   in config so the connector picks the right value.
7. **OAuth refresh-token revocation.** Zoho keeps roughly 20 refresh
   tokens per client_id; the 21st silently invalidates the oldest.
   Generate a fresh refresh_token in `api-console.zoho.{dc}` and
   re-deploy if you see 401s.
8. **Multi-DC token mismatch.** Tokens generated against
   `accounts.zoho.com` will not work against `zohoapis.eu`. Make sure
   your Self Client was created in the same data center you've set in
   `configuration.json`.
9. **Availability / write endpoints are not synced.** `/availableslots`
   is transient slot data and not useful for analytics. The book / update
   / reschedule endpoints exist but this connector is read-only.

## File layout

```
zoho_bookings/
├── README.md                  this file
├── requirements.txt           fivetran-connector-sdk, requests, duckdb
├── configuration.json.example fill in and copy to configuration.json
├── connector.py               schema() + update() entry points
├── auth.py                    OAuth refresh-token cache + data-center routing
├── api_client.py              rate limiter, GET + POST form-data, paginated appointments
├── helpers.py                 flattening, upsert wrapper, hard-delete reconciliation
├── schema.py                  static table list (9 tables)
├── tables_meta.py             upsert workspaces / services / staff / resources + bridge rows
├── tables_data.py             paginated appointments, full re-sync, hard-delete diff
└── validate.py                Apple-Silicon local-test harness (DuckDB native)
```
