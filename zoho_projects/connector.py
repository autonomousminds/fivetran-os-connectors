"""
Fivetran Custom Connector for Zoho Projects (https://www.zoho.com/projects/).

Pulls everything the OAuth grant can reach across one or more portals into a
Fivetran destination — projects (active + archived + template), milestones,
tasklists, tasks (with subtasks, comments, attachments, followers,
dependencies, activities, status history), bugs (full refresh, with the same
fan-out), portal-wide time logs, events, forums, documents, users, clients,
client users, leaves, plus all the metadata tables (modules, fields,
layouts, custom-field schemas, custom statuses, project groups, tags) and a
tag-associations junction.

Custom fields land in long-form child tables — one row per
`(parent_id, field_api_name)` keyed by the stable `UDF_<TYPE><N>` api name.

See README.md for the OAuth Self-Client setup, scope list, configuration
keys, and rate-limit notes.
"""

import json

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

import api_client
import auth
import helpers
import schema as schema_module
import tables_data
import tables_meta


def schema(configuration: dict) -> list:
    return schema_module.get_schema(configuration)


def update(configuration: dict, state: dict):
    helpers.validate_configuration(configuration)
    state = state or {}

    # Fresh per-run caches — required because Fivetran reuses the Python
    # process across invocations.
    auth.reset_caches()
    api_client.reset_rate_limiter()
    helpers.reset_tracking()
    tables_data.reset_udf_meta_cache()

    if state.get("_state_version") != helpers.STATE_VERSION:
        log.info(f"State version mismatch (have {state.get('_state_version')}, "
                 f"want {helpers.STATE_VERSION}) — resetting state.")
        state.clear()
        state["_state_version"] = helpers.STATE_VERSION

    log.info("Zoho Projects connector starting...")

    def _step(name: str, fn, *args, critical: bool = False):
        """Run one sync step. Logs and continues on any non-DailyLimit
        failure (so a single broken endpoint doesn't crash the whole sync),
        unless `critical=True` — those re-raise."""
        try:
            result = fn(*args)
            op.checkpoint(state=state)
            return result
        except api_client.DailyLimitExceeded:
            raise
        except Exception as e:
            if critical:
                log.severe(f"{name}: critical step failed — aborting. {e!r}")
                raise
            log.severe(f"{name}: failed with {e!r}. Continuing with next step.")
            try:
                op.checkpoint(state=state)
            except Exception:
                pass
            return None

    status_filters = (
        helpers.config_list(configuration, "project_status_filters")
        or ["active", "archived", "template"]
    )

    try:
        # 1) Portal discovery — foundational. If this fails, abort.
        portal_ids = _step("sync_portals", tables_meta.sync_portals,
                           configuration, state, critical=True)
        if not portal_ids:
            log.warning("No portals to sync — exiting.")
            op.checkpoint(state=state)
            return

        # 2) Per-portal sync
        for portal_id in portal_ids:
            log.info(f"=== Portal {portal_id} ===")

            # ── 2a) Portal-level metadata ───────────────────────────────
            module_api_names = _step(
                f"sync_modules[{portal_id}]", tables_meta.sync_modules,
                configuration, state, portal_id) or []
            _step(f"sync_fields[{portal_id}]", tables_meta.sync_fields,
                  configuration, state, portal_id, module_api_names)

            _step(f"sync_project_layouts[{portal_id}]",
                  tables_meta.sync_project_layouts,
                  configuration, state, portal_id)
            _step(f"sync_task_layouts[{portal_id}]",
                  tables_meta.sync_task_layouts,
                  configuration, state, portal_id)

            _step(f"sync_project_custom_fields_meta[{portal_id}]",
                  tables_meta.sync_project_custom_fields_meta,
                  configuration, state, portal_id)
            _step(f"sync_task_custom_fields_meta[{portal_id}]",
                  tables_meta.sync_task_custom_fields_meta,
                  configuration, state, portal_id)
            _step(f"sync_timesheet_custom_fields_meta[{portal_id}]",
                  tables_meta.sync_timesheet_custom_fields_meta,
                  configuration, state, portal_id)
            _step(f"sync_project_custom_statuses[{portal_id}]",
                  tables_meta.sync_project_custom_statuses,
                  configuration, state, portal_id)
            _step(f"sync_project_groups[{portal_id}]",
                  tables_meta.sync_project_groups,
                  configuration, state, portal_id)
            _step(f"sync_tags[{portal_id}]", tables_meta.sync_tags,
                  configuration, state, portal_id)

            # V3-only portal-wide dim tables (skip silently if not provisioned)
            _step(f"sync_profiles[{portal_id}]", tables_data.sync_profiles,
                  configuration, state, portal_id)
            _step(f"sync_roles[{portal_id}]", tables_data.sync_roles,
                  configuration, state, portal_id)
            _step(f"sync_teams_portal[{portal_id}]",
                  tables_data.sync_teams_portal,
                  configuration, state, portal_id)
            _step(f"sync_phases_portal[{portal_id}]",
                  tables_data.sync_phases_portal,
                  configuration, state, portal_id)

            # ── 2b) Portal-wide data ─────────────────────────────────────
            _step(f"sync_users[{portal_id}]", tables_data.sync_users,
                  configuration, state, portal_id)
            client_ids = _step(
                f"sync_clients[{portal_id}]", tables_data.sync_clients,
                configuration, state, portal_id) or []
            _step(f"sync_client_users[{portal_id}]",
                  tables_data.sync_client_users,
                  configuration, state, portal_id, client_ids)
            _step(f"sync_leaves[{portal_id}]", tables_data.sync_leaves,
                  configuration, state, portal_id)
            _step(f"sync_time_logs[{portal_id}]", tables_data.sync_time_logs,
                  configuration, state, portal_id)

            # ── 2c) Projects (three status passes, LMT-filtered) ────────
            project_result = _step(
                f"sync_projects[{portal_id}]", tables_data.sync_projects,
                configuration, state, portal_id, status_filters)
            if not project_result:
                continue
            active_project_ids, all_project_ids = project_result

            # ── 2d) Per-project metadata that depends on the project list ─
            _step(f"sync_timesheet_layouts[{portal_id}]",
                  tables_meta.sync_timesheet_layouts,
                  configuration, state, portal_id, all_project_ids)
            _step(f"sync_bug_custom_fields_meta[{portal_id}]",
                  tables_meta.sync_bug_custom_fields_meta,
                  configuration, state, portal_id, all_project_ids)
            _step(f"sync_bug_default_fields[{portal_id}]",
                  tables_meta.sync_bug_default_fields,
                  configuration, state, portal_id, all_project_ids)
            _step(f"sync_bug_renamed_fields[{portal_id}]",
                  tables_meta.sync_bug_renamed_fields,
                  configuration, state, portal_id, all_project_ids)

            # ── 2e) Per-project data fan-out ─────────────────────────────
            sync_documents_flag = helpers.config_bool(
                configuration, "sync_documents", True)
            sync_activities_flag = helpers.config_bool(
                configuration, "sync_activities_feeds", True)
            sync_task_status_history_flag = helpers.config_bool(
                configuration, "sync_task_status_history", True)

            for project_id in all_project_ids:
                _step(f"sync_project_users[{portal_id}/{project_id}]",
                      tables_data.sync_project_users,
                      configuration, state, portal_id, project_id)
                _step(f"sync_project_clients[{portal_id}/{project_id}]",
                      tables_data.sync_project_clients,
                      configuration, state, portal_id, project_id)
                _step(f"sync_milestones[{portal_id}/{project_id}]",
                      tables_data.sync_milestones,
                      configuration, state, portal_id, project_id)
                _step(f"sync_tasklists[{portal_id}/{project_id}]",
                      tables_data.sync_tasklists,
                      configuration, state, portal_id, project_id)
                _step(f"sync_tasks[{portal_id}/{project_id}]",
                      tables_data.sync_tasks,
                      configuration, state, portal_id, project_id)
                _step(f"sync_task_custom_views[{portal_id}/{project_id}]",
                      tables_data.sync_task_custom_views,
                      configuration, state, portal_id, project_id)
                if sync_task_status_history_flag:
                    _step(
                        f"sync_task_status_history[{portal_id}/{project_id}]",
                        tables_data.sync_task_status_history,
                        configuration, state, portal_id, project_id)
                _step(f"sync_bugs[{portal_id}/{project_id}]",
                      tables_data.sync_bugs,
                      configuration, state, portal_id, project_id)
                _step(f"sync_bug_custom_views[{portal_id}/{project_id}]",
                      tables_data.sync_bug_custom_views,
                      configuration, state, portal_id, project_id)
                _step(f"sync_events[{portal_id}/{project_id}]",
                      tables_data.sync_events,
                      configuration, state, portal_id, project_id)
                _step(f"sync_forums[{portal_id}/{project_id}]",
                      tables_data.sync_forums,
                      configuration, state, portal_id, project_id)
                if sync_documents_flag:
                    _step(f"sync_documents[{portal_id}/{project_id}]",
                          tables_data.sync_documents,
                          configuration, state, portal_id, project_id)
                if sync_activities_flag:
                    _step(
                        f"sync_project_activities[{portal_id}/{project_id}]",
                        tables_data.sync_project_activities,
                        configuration, state, portal_id, project_id)
                    _step(
                        f"sync_project_statuses[{portal_id}/{project_id}]",
                        tables_data.sync_project_statuses,
                        configuration, state, portal_id, project_id)
                _step(
                    f"sync_project_comments[{portal_id}/{project_id}]",
                    tables_data.sync_project_comments,
                    configuration, state, portal_id, project_id)
                _step(
                    f"sync_project_teams[{portal_id}/{project_id}]",
                    tables_data.sync_project_teams,
                    configuration, state, portal_id, project_id)
                _step(
                    f"sync_phases_for_project[{portal_id}/{project_id}]",
                    tables_data.sync_phases_for_project,
                    configuration, state, portal_id, project_id)

        log.info("Zoho Projects connector sync complete.")
        op.checkpoint(state=state)

    except api_client.DailyLimitExceeded as exc:
        # Soft exit: checkpoint whatever we have so the next run resumes
        # without redoing work.
        log.severe(str(exc))
        op.checkpoint(state=state)


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
