"""
Appointment sync for the Zoho Bookings connector.

Strategy: **pure full re-sync each run** over a configurable date window.
This is simple and always captures status changes (cancellations,
reschedules, completions). The cost is O(appointments in window) API
calls per sync.

Date window:
  from_time = today - appointments_past_window_days   (default 365)
  to_time   = today + appointments_future_window_days (default 365)

Both bounds are inclusive; Zoho's documented format is `dd-MMM-yyyy HH:mm:ss`.

Hard deletes (appointments removed from Bookings entirely) are reconciled
against the previous run's ID snapshot.
"""

from datetime import datetime, timedelta, timezone

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import fetch_paginated_appointments
from helpers import (
    flatten_record_auto, ids_seen, reconcile_deletes, upsert,
)


CHECKPOINT_EVERY = 1000


def _format_zoho_datetime(dt: datetime) -> str:
    """`dd-MMM-yyyy HH:mm:ss` (e.g. 01-Jan-2025 00:00:00). Zoho's documented
    format for fetchappointment from_time/to_time."""
    return dt.strftime("%d-%b-%Y %H:%M:%S")


def _resolve_window(config: dict) -> tuple[str, str]:
    past_days = int(config.get("appointments_past_window_days") or 365)
    future_days = int(config.get("appointments_future_window_days") or 365)
    now = datetime.now(timezone.utc)
    return (
        _format_zoho_datetime(now - timedelta(days=past_days)),
        _format_zoho_datetime(now + timedelta(days=future_days)),
    )


def _resolve_per_page(config: dict) -> int:
    """100 normally, but Zoho caps at 60 when custom fields are enabled."""
    if str(config.get("bookings_has_custom_fields", "")).lower() in ("true", "1", "yes"):
        return min(60, int(config.get("appointments_per_page") or 60))
    return min(100, int(config.get("appointments_per_page") or 100))


def sync_appointments(config: dict, state: dict, workspace_ids: list):
    from_time, to_time = _resolve_window(config)
    per_page = _resolve_per_page(config)
    # /fetchappointment ignores workspace_id as a form param; the endpoint
    # always returns appointments across every workspace the token can see.
    # Filter client-side using the `workspace_id` echoed on each appointment.
    allowed = set(str(w) for w in (workspace_ids or []))
    log.info(f"appointments: full re-sync window [{from_time} → {to_time}] "
             f"per_page={per_page}, workspace filter={'all' if not allowed else sorted(allowed)}")

    n_total = 0
    n_filtered_out = 0
    for record in fetch_paginated_appointments(
        config,
        from_time=from_time,
        to_time=to_time,
        per_page=per_page,
    ):
        record_workspace = str(record.get("workspace_id") or "")
        if allowed and record_workspace and record_workspace not in allowed:
            n_filtered_out += 1
            continue

        row = flatten_record_auto(
            record,
            nested_prefix_keys={"customer_more_info"},
        )
        if "booking_id" not in row and "id" in row:
            row["booking_id"] = row["id"]
        if not row.get("booking_id"):
            continue
        upsert("appointments", row, id_key="booking_id")
        n_total += 1
        if n_total % CHECKPOINT_EVERY == 0:
            op.checkpoint(state=state)
            log.fine(f"appointments: checkpointed at {n_total} rows")

    if n_filtered_out:
        log.info(f"appointments: filtered out {n_filtered_out} record(s) "
                 f"belonging to non-configured workspaces")
    log.info(f"appointments: upserted {n_total} total")
    reconcile_deletes("appointments", ids_seen("appointments"), state,
                      key_template="booking_id")
