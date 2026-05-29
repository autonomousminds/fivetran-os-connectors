"""
Fivetran Custom Connector for Zoho People (https://www.zoho.com/people/).

Pulls everything the OAuth grant can reach into a Fivetran destination —
employees + departments + designations + locations + every other Zoho
People form (via dynamic discovery), plus the bespoke module endpoints
for attendance, leave, timesheets, files, holidays, and optionally LMS
courses.

See README.md for the OAuth Self-Client setup, scope list, configuration
keys, and the per-module rate-limit notes.
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
    """Discovers forms and returns the full table list (static + dynamic)."""
    return schema_module.get_schema(configuration)


def update(configuration: dict, state: dict):
    helpers.validate_configuration(configuration)
    state = state or {}

    # Fresh per-run caches — required because Fivetran reuses the Python
    # process across invocations.
    auth.reset_caches()
    api_client.reset_rate_limiter()
    helpers.reset_tracking()
    schema_module.reset_discovery_cache()
    tables_data.reset_dedup_caches()

    if state.get("_state_version") != helpers.STATE_VERSION:
        log.info(f"State version mismatch (have {state.get('_state_version')}, "
                 f"want {helpers.STATE_VERSION}) — resetting state.")
        state.clear()
        state["_state_version"] = helpers.STATE_VERSION

    log.info("Zoho People connector starting...")

    def _step(name: str, fn, *args, critical: bool = False):
        """Run one sync step. Logs and continues on any non-DailyLimit
        failure (so a single broken module doesn't crash the whole sync),
        unless `critical=True` — those re-raise."""
        try:
            fn(*args)
            op.checkpoint(state=state)
        except api_client.DailyLimitExceeded:
            raise  # always propagate quota-exhaustion
        except Exception as e:
            if critical:
                log.severe(f"{name}: critical step failed — aborting. {e!r}")
                raise
            log.severe(f"{name}: failed with {e!r}. Continuing with next step.")
            try:
                op.checkpoint(state=state)
            except Exception:
                pass

    try:
        # 1) Discovery + forms_meta are foundational. If they fail, abort —
        #    the dynamic per-form sync depends on the catalog.
        _step("sync_forms_meta", tables_meta.sync_forms_meta,
              configuration, state, critical=True)
        _step("sync_form_fields", tables_meta.sync_form_fields,
              configuration, state)

        # 2) Small reference tables — each may fail independently (scope gaps).
        _step("sync_file_categories", tables_meta.sync_file_categories,
              configuration, state)
        _step("sync_files", tables_meta.sync_files,
              configuration, state)
        _step("sync_holidays", tables_meta.sync_holidays,
              configuration, state)

        # 3) Per-form record sync (employees, departments, etc.) — already
        #    has per-form try/except inside, but wrap the orchestrator too.
        _step("sync_all_forms", tables_data.sync_all_forms,
              configuration, state)

        # 4) Attendance — org-wide daily summary
        _step("sync_attendance_daily", tables_data.sync_attendance_daily,
              configuration, state)

        # 5) Leave — records, balance snapshots, types
        _step("sync_leave_records", tables_data.sync_leave_records,
              configuration, state)
        _step("sync_leave_balance", tables_data.sync_leave_balance,
              configuration, state)

        # 6) Time tracker — jobs + timelogs + clients + projects + timesheets
        _step("sync_jobs", tables_data.sync_jobs, configuration, state)
        _step("sync_timelogs", tables_data.sync_timelogs, configuration, state)
        _step("sync_timetracker_clients",
              tables_data.sync_timetracker_clients, configuration, state)
        _step("sync_timetracker_projects",
              tables_data.sync_timetracker_projects, configuration, state)
        _step("sync_timesheets",
              tables_data.sync_timesheets, configuration, state)

        # 7) Attendance enrichment beyond daily summary
        _step("sync_attendance_regularization",
              tables_data.sync_attendance_regularization, configuration, state)
        _step("sync_shift_mappings",
              tables_data.sync_shift_mappings, configuration, state)
        _step("sync_attendance_latest_entries",
              tables_data.sync_attendance_latest_entries, configuration, state)
        _step("sync_attendance_entries",
              tables_data.sync_attendance_entries, configuration, state)

        # 8) HR ops + comms
        _step("sync_cases", tables_data.sync_cases, configuration, state)
        _step("sync_announcements",
              tables_data.sync_announcements, configuration, state)

        # 9) LMS — courses + per-learner progress (opt-in)
        _step("sync_courses", tables_data.sync_courses, configuration, state)
        _step("sync_learner_progress",
              tables_data.sync_learner_progress, configuration, state)

        log.info("Zoho People connector sync complete.")
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
