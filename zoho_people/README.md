# Zoho People — Fivetran Custom Connector

Syncs **everything the OAuth grant can reach** out of
[Zoho People](https://www.zoho.com/people/) into a Fivetran destination:
employees, departments, designations, locations, every custom HR form,
attendance, leave, timesheets, files, holidays, and (optionally) LMS
courses.

Discovery is **dynamic**: the connector calls `/people/api/forms` at the
start of every run, builds one data table per form it finds, and picks
up new forms automatically as Zoho admins add them. The companion module
tables (attendance, leave, timesheet, etc.) are fixed.

Built against the **Zoho People REST API** (v1 + v2 endpoints, plus the
newer `/api/forms/{view}/records` surface). Documentation:
<https://www.zoho.com/people/api/overview.html>.

Modeled after the sister `zoho_creator` / `zoho_bookings` connectors and
shares their `auth.py` pattern, sliding-window rate limiter, and Apple-
Silicon validation harness.

## Tables

### Fixed meta tables

| Table             | Primary key                          | Source endpoint                                        |
|-------------------|--------------------------------------|--------------------------------------------------------|
| `forms_meta`      | `form_link_name`                     | `GET /people/api/forms`                                |
| `views_meta`      | `form_link_name`, `view_name`        | `GET /api/forms/{form}/views`                          |
| `form_fields`     | `form_link_name`, `field_label`      | `GET /api/forms/{form}/components` (best-effort)       |
| `file_categories` | `file_category_id`                   | `GET /people/api/files/getCategories`                  |
| `files`           | `file_id`                            | `GET /people/api/files/getAllFiles` (HR + Company)     |
| `holidays`        | `id`                                 | `GET /people/api/leave/v2/holidays/get`                |

### Transactional tables

| Table              | Primary key                                              | Source endpoint                                          |
|--------------------|----------------------------------------------------------|----------------------------------------------------------|
| `attendance_daily` | `employee_id`, `date`                                    | `GET /people/api/attendance/getUserReport`               |
| `attendance_regularization` | `record_id`                                     | `GET /people/api/attendance/getRegularizationRecords`    |
| `shift_mappings`   | `employee_id`, `from_date`, `shift_name`                 | `GET /people/api/attendance/getShiftConfiguration` (per-employee) |
| `leave_records`    | `id`                                                     | `GET /people/api/v2/leavetracker/leaves/records`         |
| `leave_records_days` | `leave_id`, `date`                                     | derived (per-day breakdown from `leave_records.Days`)    |
| `leave_balance`    | `employee_id`, `leave_type_id`, `from_date`, `to_date`   | `GET /people/api/v2/leavetracker/reports/bookedAndBalance` |
| `leave_types`      | `id`                                                     | derived from `leave_records` + `leave_balance`           |
| `jobs`             | `job_id`                                                 | `GET /people/api/timetracker/getjobs`                    |
| `timelogs`         | `timelog_id`                                             | `GET /people/api/timetracker/gettimelogs` (monthly chunks) |
| `timesheets`       | `timesheet_id`                                           | `GET /people/api/timetracker/gettimesheet` (monthly chunks) |
| `timetracker_clients`  | `client_id`                                          | `GET /people/api/timetracker/getclients`                 |
| `timetracker_projects` | `project_id`                                         | `GET /people/api/timetracker/getprojects`                |
| `cases`            | `case_id`                                                | `GET /api/hrcases/getAllCases`                           |
| `announcements`    | `announcement_id`                                        | `GET /people/api/announcement/getAllAnnouncement`        |
| `courses`          | `course_id` (optional)                                   | `GET /api/v1/courses` (gated on `sync_lms_courses`)      |
| `learner_progress` | `learner_id`, `course_id` (optional)                     | `GET /api/v1/learners/{id}/course-progress` (per-employee, gated on `sync_lms_courses`) |
| `attendance_latest_entries` | `employee_id`, `date`, `entry_idx` (optional)   | `GET /api/attendance/fetchLatestAttEntries` (gated on `sync_attendance_latest_entries`) |
| `attendance_entries` | `employee_id`, `date` (optional)                       | `GET /people/api/attendance/getAttendanceEntries` (per-employee per-day, gated on `sync_attendance_entries`) |
| `attendance_entries_punches` | `employee_id`, `date`, `punch_idx` (optional)  | derived from `attendance_entries.entries`                |

### Dynamic per-form tables

For every form discovered, one table:

```
form_{safe(form_link_name)}        primary key: record_id
```

For Zoho's standard HR forms this produces tables like:

```
form_p_employee              employees
form_p_department            departments
form_p_designation           designations
form_p_location              locations
form_p_dependants            dependants
form_p_education             education history
form_p_workexperience        prior work experience
form_p_assets                asset assignments
```

…plus one table per custom form your Zoho People admins have built.

Plus one child table per **tabular section** found inside a form record
(work-experience rows on an employee, dependants on an employee, etc.):

```
form_p_employee__sub_workexperience      primary key: record_id (FK: parent_record_id)
form_p_employee__sub_dependants          primary key: record_id (FK: parent_record_id)
```

Subform child tables are not pre-declared — the SDK creates them on
first upsert from real data and infers columns automatically.

## Sync strategy

- **Per-form data tables**:
  - First run / weekly full re-sync → full pull via the default view's
    `/api/forms/{view}/records` endpoint (no `modifiedtime` filter).
  - Subsequent runs → same endpoint filtered by
    `modifiedtime > {last_run_ms}` (Zoho takes the cursor in milliseconds
    since epoch).
  - Every 7 days per form → a full re-sync runs to catch hard-deleted
    records (the `modifiedtime` filter never returns deleted IDs).
    Missing IDs are emitted as `op.delete`.
- **Attendance daily**: full pull of the configured past window every
  run. Idempotent because primary key is `(employee_id, date)` and Zoho
  lets users regularise old days, so a fresh window catches edits.
- **Leave records**: full pull of `[past_window, future_window]` every
  run. Idempotent on `id`.
- **Leave balance**: full snapshot every run, keyed on
  `(employee_id, leave_type_id, from_date, to_date)` so a window change
  keeps prior snapshots queryable.
- **Jobs / timelogs**: full pull each run (jobs are small; timelogs are
  walked in monthly chunks because Zoho caps a single timelog query at
  1 month).
- **Holidays**: full pull of `[-past, +future]` window each run.

## OAuth Self-Client setup (one-time)

1. Visit `https://api-console.zoho.{dc}` for your data center
   (`eu`, `com`, `in`, `com.au`, `com.cn`, `jp`, `sa`, `cloud.ca`).
2. Create a new **Self Client** (not Server-based, not Mobile). Copy the
   `Client ID` and `Client Secret`.
3. Under the Self Client → **Generate Code** tab, paste these scopes
   (space-separated):

   ```
   ZOHOPEOPLE.forms.READ ZOHOPEOPLE.employee.ALL ZOHOPEOPLE.attendance.ALL ZOHOPEOPLE.leave.READ ZOHOPEOPLE.timetracker.READ
   ```

   Add `ZOHOPEOPLE.training.READ` too if you want LMS courses. Click
   **Create**, copy the grant code (10-minute expiry — fine, only used
   once).
4. Exchange the grant code for a `refresh_token` (substitute your data
   center's accounts host):

   ```bash
   curl -X POST 'https://accounts.zoho.eu/oauth/v2/token' \
     -d 'grant_type=authorization_code' \
     -d 'client_id=YOUR_CLIENT_ID' \
     -d 'client_secret=YOUR_CLIENT_SECRET' \
     -d 'code=YOUR_GRANT_CODE'
   ```

   The response carries `"refresh_token": "1000.xxx..."`. **Store it in
   a password manager** — Zoho only shows it once.
5. Fill in `configuration.json` (copy from `configuration.json.example`).

## Configuration

```json
{
    "client_id":                       "1000.XXXXXXXXXXXXXXXXXX",
    "client_secret":                   "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "refresh_token":                   "1000.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.xxxxxxxxxxxx",
    "data_center":                     "eu",
    "attendance_past_window_days":     "180",
    "leave_past_window_days":          "365",
    "leave_future_window_days":        "365",
    "timelog_past_window_days":        "180",
    "holidays_past_window_days":       "365",
    "holidays_future_window_days":     "730",
    "sync_lms_courses":                "false",
    "skip_forms":                      []
}
```

- `data_center` ∈ `{com, eu, in, com.au, com.cn, jp, sa, ca}`.
- `attendance_past_window_days` — how many days of daily attendance
  summary (and regularization records, and shift mappings) to pull each
  run. Default 180.
- `leave_past_window_days` / `leave_future_window_days` — leave
  application window. Default 365 / 365.
- `timelog_past_window_days` — how far back to walk timelogs (monthly
  chunks). Default 180.
- `timesheets_past_window_days` — how far back to walk timesheets
  (monthly chunks). Default 180.
- `holidays_past_window_days` / `holidays_future_window_days` — holiday
  calendar window. Default 365 / 730.
- `sync_lms_courses` — set `"true"` to enable LMS courses **and**
  per-learner course progress (requires the extra
  `ZOHOPEOPLE.training.READ` scope on the refresh_token).
- `sync_attendance_entries` — set `"true"` to enable per-employee
  per-day check-in/out detail. **EXPENSIVE**: one API call per employee
  per day; with the 30-req/5-min rate limit, syncing 21 employees ×
  14 days = ~50 minutes. Off by default.
- `attendance_entries_past_window_days` — how far back to pull per-day
  entries when `sync_attendance_entries=true`. Default 14.
- `sync_attendance_latest_entries` — set `"true"` for a single-call
  recent-attendance snapshot across the org (good for live dashboards).
  Default true.
- `attendance_latest_entries_duration_minutes` — minutes-back window for
  the latest-entries snapshot. Default 1440 (24 hours).
- `skip_forms` — optional list of form link names to exclude from the
  dynamic per-form sync.

## Running locally

```bash
conda activate fivetran-productive   # already has fivetran-connector-sdk, duckdb, requests
cd zoho_people
cp configuration.json.example configuration.json
# edit configuration.json with real credentials
python validate.py                   # Apple-Silicon-safe; writes files/local_warehouse.db
```

The standard SDK debugger works on Linux / Intel macOS but **crashes on
Apple Silicon** (x86_64 JVM under Rosetta + DuckDB JNI). Use
`validate.py` locally on M-series Macs:

```bash
fivetran debug --configuration configuration.json   # NOT on Apple Silicon
fivetran deploy --api-key <KEY> --destination <DEST> --connection zoho_people --configuration configuration.json
```

Inspect local output:

```bash
duckdb files/local_warehouse.db -c "SHOW TABLES;"
duckdb files/local_warehouse.db -c "SELECT COUNT(*) FROM form_p_employee;"
duckdb files/local_warehouse.db -c "SELECT employee_id, first_name, last_name, employee_email FROM form_p_employee LIMIT 10;"
duckdb files/local_warehouse.db -c "SELECT employee_id, date, total_hours, status FROM attendance_daily ORDER BY date DESC LIMIT 20;"
duckdb files/local_warehouse.db -c "SELECT employee_name, leave_type_name, from_date, to_date, approval_status FROM leave_records ORDER BY from_date DESC LIMIT 20;"
duckdb files/local_warehouse.db -c "SELECT client_name, project_name, job_name, work_date, hours FROM timelogs ORDER BY work_date DESC LIMIT 20;"
```

## Caveats (handled in the connector, but worth knowing)

1. **Tight rate limits.** Zoho People publishes most read endpoints at
   `30 requests / 5 minutes` per endpoint, with a 5-minute lockout if
   you exceed it. Our limiter caps at **25 / 5 min per endpoint** with a
   `200 / 5 min` global ceiling. Big tenants may see the connector pause
   for several minutes between requests — that's by design.

2. **No `modifiedtime` on most module endpoints.** Attendance, leave,
   timelogs, holidays, and files don't support a "modified since"
   filter. We re-pull the configured past window every run; all of
   these tables are idempotent on their PKs.

3. **Timelogs are capped at 1 month per query.** The connector walks
   the past window in 30-day chunks. A 180-day default means 6 chunks
   per run — combined with the 20/5min rate limit on
   `getjobs`/`gettimelogs`, this is the slowest part of the sync.

4. **Forms list is the source of truth.** If `/people/api/forms` doesn't
   include a form (visibility or permissions), the connector can't sync
   it — even if the underlying records exist. Check `forms_meta` in
   the destination to see which forms got picked up. Set
   `skip_forms: ["<linkName>"]` to exclude noisy ones.

5. **Default view fallback.** Forms with no discoverable views get the
   guessed convention `{FormLinkName}View` as a default. The standard
   forms (`P_employee`, `P_Department`, etc.) all have well-known views
   by this name, so the fallback works in practice. Custom forms always
   expose their views via `/api/forms/{form}/views`.

6. **Hard deletes** are reconciled only on the weekly full re-sync per
   form (the `modifiedtime` filter never returns deleted IDs). If you
   need same-day delete propagation, drop `FULL_SYNC_INTERVAL_SECONDS`
   in `tables_data.py`.

7. **OAuth refresh-token revocation.** Zoho keeps roughly 20 refresh
   tokens per client_id; the 21st silently invalidates the oldest.
   Regenerate the refresh_token at `api-console.zoho.{dc}` and
   re-deploy if 401s appear.

8. **Multi-DC token mismatch.** A token generated against
   `accounts.zoho.com` will not work against `people.zoho.eu`. Make
   sure your Self Client was created in the same data center you've
   set in `configuration.json`.

9. **Scope errors are skip-not-fail.** If the refresh_token is missing
   any of the optional scopes (e.g. `training.READ`), the connector
   logs a warning and skips that module instead of aborting. Forms,
   employees, attendance, leave, and timetracker are essentially
   non-optional — missing scopes there will produce empty tables
   downstream.

10. **File uploads.** We store the file's URL, name, category, and
    metadata but do NOT download the binary content.

11. **LMS / training.** Disabled by default. Set `sync_lms_courses` to
    `"true"` and add `ZOHOPEOPLE.training.READ` to the refresh_token
    scopes if you want it.

## File layout

```
zoho_people/
├── README.md                  this file
├── requirements.txt           fivetran-connector-sdk, requests, duckdb
├── configuration.json.example fill in and copy to configuration.json
├── connector.py               schema() + update() entry points
├── auth.py                    OAuth refresh-token cache + data-center routing
├── api_client.py              rate limiter, paginated GET, envelope unwrap
├── helpers.py                 flatten/extract, upsert wrapper, hard-delete reconciliation
├── schema.py                  static + dynamic schema, form discovery
├── tables_meta.py             forms_meta, views_meta, file_categories, files, holidays
├── tables_data.py             per-form records, attendance, leave, timetracker, courses
└── validate.py                Apple-Silicon local-test harness (DuckDB native)
```
