"""
Schema for the Zoho Projects connector.

Unlike Zoho Creator / Zoho People — both of which discover dynamic per-form
tables at runtime — Zoho Projects has a fixed resource model: portals,
projects, milestones, tasklists, tasks, bugs, time logs, events, forums,
documents, users, clients, leaves, plus their custom-field child tables.
So the table list is static.

Multi-portal: every table has `portal_id` as a column (and as part of the
composite primary key where appropriate) so the warehouse can multi-tenant
cleanly when the OAuth grant exposes more than one portal.

Custom fields are modelled as long-form child tables per parent — one row
per `(parent_id, field_api_name)` keyed by the stable Zoho UDF api name
(`UDF_CHAR1`, `UDF_DATE2`, etc.). Survives field renames and never causes
schema drift in the destination warehouse.

All ~50 tables are declared here. The Fivetran SDK infers columns at upsert
time from the first record per table, so we never need to enumerate them.
"""

from fivetran_connector_sdk import Logging as log


def get_schema(configuration: dict) -> list:
    from helpers import config_bool, validate_configuration
    validate_configuration(configuration)

    sync_documents = config_bool(configuration, "sync_documents", True)
    sync_attachments_meta = config_bool(configuration, "sync_attachments_meta", True)
    sync_activities_feeds = config_bool(configuration, "sync_activities_feeds", True)
    sync_task_status_history = config_bool(configuration, "sync_task_status_history", True)
    sync_bug_task_associations = config_bool(configuration, "sync_bug_task_associations", True)

    schema_list: list = [
        # ── Portal-level metadata (tables_meta) ──────────────────────────
        {"table": "portals",
         "primary_key": ["portal_id"]},

        {"table": "modules",
         "primary_key": ["portal_id", "module_api_name"]},
        {"table": "fields",
         "primary_key": ["portal_id", "module_api_name", "field_api_name"]},

        {"table": "project_layouts",
         "primary_key": ["portal_id", "layout_id"]},
        {"table": "task_layouts",
         "primary_key": ["portal_id", "layout_id"]},
        {"table": "timesheet_layouts",
         "primary_key": ["portal_id", "project_id"]},

        {"table": "project_custom_fields_meta",
         "primary_key": ["portal_id", "field_api_name"]},
        {"table": "task_custom_fields_meta",
         "primary_key": ["portal_id", "field_api_name"]},
        {"table": "bug_custom_fields_meta",
         "primary_key": ["portal_id", "project_id", "field_api_name"]},
        {"table": "timesheet_custom_fields_meta",
         "primary_key": ["portal_id", "field_api_name"]},

        {"table": "project_custom_statuses",
         "primary_key": ["portal_id", "status_id"]},

        {"table": "bug_default_fields",
         "primary_key": ["portal_id", "project_id", "field_name"]},
        {"table": "bug_renamed_fields",
         "primary_key": ["portal_id", "project_id", "field_name"]},

        {"table": "project_groups",
         "primary_key": ["portal_id", "group_id"]},

        {"table": "tags",
         "primary_key": ["portal_id", "tag_id"]},

        # ── People & clients (portal-wide, full refresh) ─────────────────
        {"table": "users",
         "primary_key": ["portal_id", "user_id"]},
        {"table": "clients",
         "primary_key": ["portal_id", "client_id"]},
        {"table": "client_users",
         "primary_key": ["portal_id", "client_user_id"]},
        {"table": "leaves",
         "primary_key": ["portal_id", "leave_id"]},

        # ── Projects hierarchy (tables_data) ─────────────────────────────
        {"table": "projects",
         "primary_key": ["portal_id", "project_id"]},
        {"table": "project_users",
         "primary_key": ["portal_id", "project_id", "user_id"]},
        {"table": "project_clients",
         "primary_key": ["portal_id", "project_id", "client_id"]},
        {"table": "project_custom_fields",
         "primary_key": ["portal_id", "project_id", "field_api_name"]},

        {"table": "milestones",
         "primary_key": ["portal_id", "project_id", "milestone_id"]},

        {"table": "tasklists",
         "primary_key": ["portal_id", "project_id", "tasklist_id"]},

        {"table": "tasks",
         "primary_key": ["portal_id", "project_id", "task_id"]},
        {"table": "task_custom_fields",
         "primary_key": ["portal_id", "project_id", "task_id", "field_api_name"]},
        {"table": "subtasks",
         "primary_key": ["portal_id", "project_id", "task_id", "subtask_id"]},
        {"table": "task_comments",
         "primary_key": ["portal_id", "project_id", "task_id", "comment_id"]},
        {"table": "task_followers",
         "primary_key": ["portal_id", "project_id", "task_id", "user_id"]},
        {"table": "task_dependencies",
         "primary_key": ["portal_id", "project_id", "task_id",
                         "depends_on_task_id"]},
        {"table": "task_owners",
         "primary_key": ["portal_id", "project_id", "task_id", "user_id"]},
        {"table": "task_activities",
         "primary_key": ["portal_id", "project_id", "task_id", "activity_id"]},
        {"table": "task_custom_views",
         "primary_key": ["portal_id", "project_id", "view_id"]},

        # ── Bugs (full refresh per run) ──────────────────────────────────
        {"table": "bugs",
         "primary_key": ["portal_id", "project_id", "bug_id"]},
        {"table": "bug_custom_fields",
         "primary_key": ["portal_id", "project_id", "bug_id", "field_api_name"]},
        {"table": "bug_comments",
         "primary_key": ["portal_id", "project_id", "bug_id", "comment_id"]},
        {"table": "bug_resolutions",
         "primary_key": ["portal_id", "project_id", "bug_id"]},
        {"table": "bug_timers",
         "primary_key": ["portal_id", "project_id", "bug_id"]},
        {"table": "bug_followers",
         "primary_key": ["portal_id", "project_id", "bug_id", "user_id"]},
        {"table": "bug_activities",
         "primary_key": ["portal_id", "project_id", "bug_id", "activity_id"]},
        {"table": "bug_custom_views",
         "primary_key": ["portal_id", "project_id", "view_id"]},

        # ── Time tracking ────────────────────────────────────────────────
        {"table": "time_logs",
         "primary_key": ["portal_id", "log_id"]},
        {"table": "timesheet_custom_fields",
         "primary_key": ["portal_id", "log_id", "field_api_name"]},

        # ── Events / forums ──────────────────────────────────────────────
        {"table": "events",
         "primary_key": ["portal_id", "project_id", "event_id"]},
        {"table": "forums",
         "primary_key": ["portal_id", "project_id", "forum_id"]},
        {"table": "forum_categories",
         "primary_key": ["portal_id", "project_id", "category_id"]},
        {"table": "forum_comments",
         "primary_key": ["portal_id", "project_id", "forum_id", "comment_id"]},
        {"table": "forum_followers",
         "primary_key": ["portal_id", "project_id", "forum_id", "user_id"]},
        {"table": "forum_attachments",
         "primary_key": ["portal_id", "project_id", "forum_id", "attachment_id"]},

        # ── Project-wall comments + event comments ───────────────────────
        {"table": "project_comments",
         "primary_key": ["portal_id", "project_id", "comment_id"]},
        {"table": "event_comments",
         "primary_key": ["portal_id", "project_id", "event_id", "comment_id"]},

        # ── Permissions & org structure (V3 — graceful skip if not provisioned)
        {"table": "profiles",
         "primary_key": ["portal_id", "profile_id"]},
        {"table": "roles",
         "primary_key": ["portal_id", "role_id"]},
        {"table": "teams",
         "primary_key": ["portal_id", "team_id"]},
        {"table": "project_teams",
         "primary_key": ["portal_id", "project_id", "team_id"]},

        # ── Phases (V3) — newer scheduling construct
        {"table": "phases",
         "primary_key": ["portal_id", "phase_id"]},

        # ── Tag associations ─────────────────────────────────────────────
        {"table": "tag_associations",
         "primary_key": ["portal_id", "entity_type", "entity_id", "tag_id"]},
    ]

    if sync_task_status_history:
        schema_list.append({
            "table": "task_status_history",
            "primary_key": ["portal_id", "project_id", "task_id", "history_id"],
        })

    if sync_bug_task_associations:
        schema_list.append({
            "table": "bug_task_associations",
            "primary_key": ["portal_id", "project_id", "bug_id", "task_id"],
        })

    if sync_attachments_meta:
        schema_list.append({
            "table": "task_attachments",
            "primary_key": ["portal_id", "project_id", "task_id", "attachment_id"],
        })
        schema_list.append({
            "table": "bug_attachments",
            "primary_key": ["portal_id", "project_id", "bug_id", "attachment_id"],
        })

    if sync_documents:
        schema_list.append({
            "table": "folders",
            "primary_key": ["portal_id", "project_id", "folder_id"],
        })
        schema_list.append({
            "table": "documents",
            "primary_key": ["portal_id", "project_id", "document_id"],
        })
        schema_list.append({
            "table": "document_versions",
            "primary_key": ["portal_id", "project_id", "document_id", "version_id"],
        })

    if sync_activities_feeds:
        schema_list.append({
            "table": "project_activities",
            "primary_key": ["portal_id", "project_id", "activity_id"],
        })
        schema_list.append({
            "table": "project_statuses",
            "primary_key": ["portal_id", "project_id", "status_id"],
        })

    log.info(f"Schema: {len(schema_list)} table(s) declared.")
    return schema_list
