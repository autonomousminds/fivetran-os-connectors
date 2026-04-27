"""Shared utilities for Xero connector — date conversion and upsert."""

import re
from datetime import datetime, timezone

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

XERO_DATE_RE = re.compile(r'/Date\((\d+)([+-]\d{4})?\)/')

STATE_VERSION = "2"  # bumped to force re-sync after pagination fix

# Checkpoint every N records in large paginated syncs (best practice: ~1000)
CHECKPOINT_INTERVAL = 1000

# Xero statuses that indicate a soft-deleted / voided / archived record
_DELETED_STATUSES = {"DELETED", "VOIDED", "ARCHIVED"}


def convert_xero_date(value):
    """Convert a single /Date(ms+offset)/ string to ISO 8601. Returns value unchanged if not a Xero date."""
    if isinstance(value, str):
        match = XERO_DATE_RE.match(value)
        if match:
            ms = int(match.group(1))
            dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    return value


def convert_xero_dates(data):
    """Convert all /Date(ms+offset)/ values in a dict to ISO 8601 strings."""
    for key, value in data.items():
        converted = convert_xero_date(value)
        if converted is not value:
            data[key] = converted
    return data


def upsert(table, data):
    """Upsert a record: convert dates, strip None/nested, then write."""
    convert_xero_dates(data)
    cleaned = {k: v for k, v in data.items()
               if v is not None and not isinstance(v, (dict, list))}
    try:
        op.upsert(table=table, data=cleaned)
    except Exception as e:
        problem_fields = {k: f"{type(v).__name__}={repr(v)[:80]}" for k, v in cleaned.items()}
        log.severe(f"Failed to upsert into {table}: {e}. Fields: {problem_fields}")
        raise


def soft_delete(table, pk_dict, status):
    """Emit op.delete() if the record's status indicates soft-deletion in Xero."""
    if status in _DELETED_STATUSES:
        op.delete(table=table, keys=pk_dict)


def validate_configuration(configuration: dict):
    """Validate required configuration keys. Raises ValueError on missing keys."""
    required = ["client_id", "client_secret"]
    missing = [k for k in required if not configuration.get(k)]
    if missing:
        raise ValueError(f"Missing required configuration keys: {', '.join(missing)}")
