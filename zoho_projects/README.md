# Zoho Projects Fivetran Connector SDK Connector

100% coverage of the Zoho Projects API in a flat, BI-friendly shape.
Modelled after the sibling connectors in this repo (`zoho_creator/`,
`zoho_bookings/`, `zoho_people/`).

API docs: https://projects.zoho.com/api-docs (V3 hub at
https://projectsapi.zoho.com/api-docs).

---

## What the connector syncs

Per portal (the OAuth grant may expose more than one):

**Portal metadata (small, full-sync each run)**

| Table | Notes |
|---|---|
| `portals` | One row per portal the OAuth token can see. |
| `modules` / `fields` | V3 metadata API — module + field definitions. |
| `project_layouts` / `task_layouts` / `timesheet_layouts` | Layout metadata per module (timesheet layouts are per-project). |
| `project_custom_fields_meta` / `task_custom_fields_meta` / `bug_custom_fields_meta` / `timesheet_custom_fields_meta` | Custom-field schema — `field_api_name`, `field_label`, `field_type`. Bug + timesheet UDFs are per-project / portal-scoped respectively. |
| `project_custom_statuses` | User-defined project lifecycle statuses. |
| `bug_default_fields` / `bug_renamed_fields` | Built-in bug field definitions + admin renames. |
| `project_groups` | Portal-level project groupings. |
| `tags` | Portal-wide tags. |

**People & clients (portal-wide, full refresh)**

| Table | Notes |
|---|---|
| `users` | Portal users. |
| `clients` | Client companies. |
| `client_users` | Users belonging to client companies. |
| `leaves` | V3 leave records (resource-planning analytics). |

**Projects + per-project fan-out (incremental via `last_modified_time`)**

| Table | Sync strategy |
|---|---|
| `projects` | LMT + three status passes (`active`, `archived`, `template`). Hard-delete reconciliation on the active set. |
| `project_users` / `project_clients` | Junctions per project. |
| `project_custom_fields` | Long-form child of `projects` — one row per (project, `UDF_<TYPE><N>`). |
| `milestones` | LMT, `status=all`. |
| `tasklists` | LMT, `flag=internal` (includes completed). |
| `tasks` | LMT. |
| `task_custom_fields` | Long-form child of `tasks`. |
| `subtasks` | Per-task fan-out. |
| `task_comments` | LMT per task. |
| `task_attachments` | Metadata only (no binaries). |
| `task_followers` | Extracted from Task Details. |
| `task_dependencies` | Extracted from Task Details `dependency` block. |
| `task_activities` | Per-task history. |
| `task_status_history` | V3, LMT at project scope. |

**Bugs (full refresh per run — `/bugs/` has no `last_modified_time`)**

| Table |
|---|
| `bugs`, `bug_custom_fields`, `bug_comments`, `bug_attachments`, `bug_resolutions`, `bug_timers`, `bug_followers`, `bug_activities`, `bug_task_associations` (V3) |

Hard-delete reconciliation is performed per `(portal, project)` on each
full bug refresh.

**Time tracking**

| Table | Notes |
|---|---|
| `time_logs` | Portal-wide `/logs`, LMT via `fetch_by_modifiedtime`. Union table with `component_type` ∈ {`task`, `bug`, `general`}. |
| `timesheet_custom_fields` | Long-form child. |

**Events / forums / documents (full refresh — no LMT support)**

`events`, `forums`, `forum_categories`, `forum_comments`, `folders`,
`documents`, `document_versions`.

**Activity & status feeds (append-only, max-id-seen cursor)**

`project_activities`, `project_statuses`.

**Tag associations**

`tag_associations` — `(portal_id, entity_type, entity_id, tag_id)` junction
populated as a side-effect of the project / milestone / tasklist / task /
bug / forum / status syncs.

Entity-type codes follow Zoho's documented map: `2`=PROJECT, `3`=MILESTONE,
`4`=TASKLIST, `5`=TASK, `6`=BUG, `7`=FORUM, `8`=STATUS. We persist the
human strings (e.g. `task`) for ergonomics.

---

## OAuth Self-Client setup

1. Sign in to [api-console.zoho.com](https://api-console.zoho.com/) — pick
   the **right data center**: `.com`, `.eu`, `.in`, `.com.au`, `.com.cn`,
   `.jp`, `.sa`, or `.ca` (Canada is `zohocloud.ca`).
2. **Add Client → Self Client**. Copy the `Client ID` and `Client Secret`.
3. Switch to the **Generate Code** tab. Paste the scope list (below) into
   the scope field, set time duration to 10 minutes, click **Create**.
4. Copy the generated **code** (single-use, expires in ~2 minutes).
5. From a shell:
   ```bash
   curl -X POST "https://accounts.zoho.<DC>/oauth/v2/token" \
     -d "grant_type=authorization_code" \
     -d "client_id=<CLIENT_ID>" \
     -d "client_secret=<CLIENT_SECRET>" \
     -d "code=<CODE>"
   ```
   The response contains a `refresh_token` — store it. It does not rotate
   on the Self-Client flow.

**Scope list (read-only)**

```
ZohoProjects.portals.READ,ZohoProjects.projects.READ,ZohoProjects.milestones.READ,ZohoProjects.tasklists.READ,ZohoProjects.tasks.READ,ZohoProjects.bugs.READ,ZohoProjects.timesheets.READ,ZohoProjects.events.READ,ZohoProjects.forums.READ,ZohoProjects.users.READ,ZohoProjects.clients.READ,ZohoProjects.documents.READ,ZohoProjects.search.READ,ZohoProjects.activities.READ,ZohoProjects.status.READ,ZohoProjects.tags.READ,ZohoProjects.leave.READ,ZohoPC.files.READ,ZohoSearch.securesearch.READ
```

These 19 scopes unlock every documented Zoho Projects endpoint the connector
calls. The V3-only tables (`profiles`, `roles`, `phases`) populate under these
existing scope buckets without needing dedicated scope strings.

**`teams` is a special case** — the V3 `/teams` endpoint returns
401 `INVALID_OAUTHSCOPE` for every standard scope combination. Zoho has not
publicly documented a scope that unlocks it (as of this writing); it may be
gated to enterprise plans only. The connector skips it gracefully — if Zoho
ever publishes a teams scope, add it to the list above and regenerate the
token.

Auth header uses Zoho's bespoke prefix — `Authorization: Zoho-oauthtoken
<access_token>` — **not** `Bearer`. The connector handles this; you don't
need to do anything special.

---

## Configuration

Copy `configuration.json.example` to `configuration.json` and fill it in:

```json
{
    "client_id": "1000.XXXXXXXXXXXXXXXXXXXXXX",
    "client_secret": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "refresh_token": "1000.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.xxxxxxxxxxxxxxxxxxxx",
    "data_center": "eu",

    "portal_ids": [],

    "project_status_filters": ["active", "archived", "template"],

    "activities_past_window_days": "0",

    "sync_documents": "true",
    "sync_attachments_meta": "true",
    "sync_activities_feeds": "true",
    "sync_task_status_history": "true",
    "sync_bug_task_associations": "true"
}
```

| Key | Default | Purpose |
|---|---|---|
| `client_id`, `client_secret`, `refresh_token` | — | Self-Client OAuth credentials. |
| `data_center` | `com` | Zoho data center: one of `com`, `eu`, `in`, `com.au`, `com.cn`, `jp`, `sa`, `ca`. |
| `portal_ids` | `[]` (= all) | Optional allow-list of portal IDs. Empty means "sync every portal the OAuth grant exposes." |
| `project_status_filters` | `["active", "archived", "template"]` | Which project status passes to run. Strip `template` if you don't want template projects in the warehouse. |
| `activities_past_window_days` | `"0"` (= no cap) | Sentinel for the append-only feeds. `"0"` pulls all history on first sync. |
| `sync_documents` | `"true"` | Toggle documents/folders/versions. |
| `sync_attachments_meta` | `"true"` | Toggle task + bug attachment metadata (URLs, no binaries). |
| `sync_activities_feeds` | `"true"` | Toggle `project_activities` + `project_statuses`. |
| `sync_task_status_history` | `"true"` | Toggle V3 task status history. |
| `sync_bug_task_associations` | `"true"` | Toggle V3 bug-task association junction. |

**All values must be strings** — Fivetran's `configuration.json` rejects
any non-string value (`fivetran deploy` errors out with "invalid
configuration file; all values must be strings"). For the list-valued
keys (`portal_ids`, `project_status_filters`), JSON-encode the list
inside a string: `"portal_ids": "[]"` or `"project_status_filters":
"[\"active\", \"archived\", \"template\"]"`. The connector's
`config_list()` helper parses these back into real Python lists at
runtime.

---

## Rate limits

Zoho Projects documents **200 requests per 2-minute rolling window per
endpoint per organisation** (counters are independent per endpoint path).
The connector enforces 180 req/2-min per endpoint plus a 400 req/2-min
global cap as a safety buffer. On 429 the connector sleeps for the
`Retry-After` interval and resumes. If the wait exceeds 10 minutes the
connector checkpoints and exits cleanly; Fivetran resumes on the next
scheduled run.

---

## Local validation (Apple Silicon)

The Fivetran SDK tester JAR bundles an x86_64 JVM and DuckDB JNI library
that crash under Rosetta after several thousand upserts. To validate
locally on an M-series Mac:

```bash
conda activate fivetran-productive
cd zoho_projects
python validate.py
```

`validate.py` monkey-patches the SDK's `Operations.upsert/checkpoint/delete`
to buffer in Python and then writes to a native ARM64 DuckDB file at
`files/local_warehouse.db`. Same `update()` code path as production — if
this completes cleanly, `fivetran deploy` will also work.

Inspect the resulting warehouse:

```bash
duckdb files/local_warehouse.db -c "SHOW TABLES;"
duckdb files/local_warehouse.db -c "SELECT COUNT(*) FROM projects;"
duckdb files/local_warehouse.db -c "SELECT COUNT(*) FROM tasks;"
duckdb files/local_warehouse.db -c "SELECT field_api_name, COUNT(*)
                                    FROM task_custom_fields
                                    GROUP BY 1 ORDER BY 2 DESC;"
```

`validate.py` is for local development only — production runs use the
standard SDK path.

---

## Deploy

```bash
cd zoho_projects
fivetran deploy
```

Then trigger a sync from the Fivetran dashboard and spot-check row counts.

---

## Known gotchas (documented for ops awareness)

- **V2 vs V3 paths coexist.** The connector prefers V3 where it exists
  (tags, leaves, task status history, bug-task associations,
  modules/fields) and falls back to V2 (`/restapi/...`) elsewhere. Zoho
  has announced a V2→V3 migration deadline of 2025-12-31 — most V2 paths
  are still functional but the connector will need a follow-up if any are
  retired.
- **Bug list endpoint has no `last_modified_time` filter.** We do a full
  refresh of bugs per project every run. This captures hard-deletes for
  free but is the most expensive part of the sync; budget accordingly.
- **`All Projects` returns only `status=active` by default.** The
  connector always issues three calls (active/archived/template) unless
  `project_status_filters` is narrowed. Removing `archived`/`template`
  will silently drop those projects.
- **Date formats are mixed.** Input dates use `MM-DD-YYYY` (even on EU/IN
  DCs). Output dates ship as a human string in the portal's timezone PLUS
  a UTC epoch-ms companion field (`*_long`). The connector consumes the
  `_long` companion and stamps an ISO-8601 `*_at` column alongside it.
- **Custom fields use stable `UDF_<TYPE><N>` API names.** Field labels
  may change — joins in BI tools should be on `field_api_name`, not
  `field_label`.
- **Followers and dependencies live inside Task Details JSON.** The
  connector hits the detail endpoint per task to extract these — that's
  one extra request per task. If you have a portal with tens of thousands
  of tasks you will see the rate limiter pace itself; that's expected.
- **No soft-delete signal.** A deleted task/bug simply disappears from
  list responses. Hard-delete reconciliation (`reconcile_deletes`) handles
  this for the full-refresh tables; incremental tables rely on a
  subsequent full refresh to catch deletes.
