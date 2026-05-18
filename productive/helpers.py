"""Shared utilities for Productive connector — JSON:API flattening, upsert, config."""

import json

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

STATE_VERSION = "4"


def should_full_sync(state: dict, table: str, interval_days: int = 7) -> bool:
    """Check if a table should do a full sync based on time since last sync.

    Returns True if the table has never synced or last synced more than
    interval_days ago. The caller must set the state key after a successful sync.
    """
    from datetime import datetime, timezone
    key = f"{table}_last_full_sync"
    last_sync = state.get(key)
    if not last_sync:
        return True
    try:
        last_dt = datetime.fromisoformat(last_sync)
        now = datetime.now(timezone.utc)
        return (now - last_dt).days >= interval_days
    except (ValueError, TypeError):
        return True


def mark_full_sync(state: dict, table: str):
    """Record that a full sync just completed for a table."""
    from datetime import datetime, timezone
    state[f"{table}_last_full_sync"] = datetime.now(timezone.utc).isoformat()


def validate_configuration(configuration: dict):
    """Validate required configuration keys. Raises ValueError on missing keys."""
    required = ["api_token", "organization_id"]
    missing = [k for k in required if not configuration.get(k)]
    if missing:
        raise ValueError(f"Missing required configuration keys: {', '.join(missing)}")


def flatten_jsonapi_record(record: dict) -> dict:
    """
    Convert a JSON:API record into a flat dict suitable for Fivetran upsert.

    Input:  {"id": "1", "type": "salaries", "attributes": {...}, "relationships": {...}}
    Output: {"id": "1", **attributes, "rel_name_id": "rel_id", ...}
    """
    flat = {"id": record["id"]}

    # Spread all attributes
    for key, value in record.get("attributes", {}).items():
        if isinstance(value, (dict, list)):
            # Serialize complex nested values as JSON strings
            flat[key] = json.dumps(value) if value else None
        else:
            flat[key] = value

    # Extract relationship IDs
    for rel_name, rel_data in record.get("relationships", {}).items():
        rel_inner = rel_data.get("data") if isinstance(rel_data, dict) else None
        if rel_inner is None:
            flat[f"{rel_name}_id"] = None
        elif isinstance(rel_inner, dict):
            flat[f"{rel_name}_id"] = rel_inner.get("id")
        elif isinstance(rel_inner, list):
            # Array relationships: store as JSON array of IDs
            flat[f"{rel_name}_ids"] = json.dumps([r["id"] for r in rel_inner]) if rel_inner else None

    return flat


def upsert(table: str, data: dict):
    """Upsert a record: serialize nested values as JSON, strip None, then write."""
    cleaned = {}
    for k, v in data.items():
        if v is None:
            continue
        elif isinstance(v, (dict, list)):
            # Store nested structures as JSON string columns (best practice)
            cleaned[k] = json.dumps(v)
        else:
            cleaned[k] = v
    try:
        op.upsert(table=table, data=cleaned)
    except Exception as e:
        problem_fields = {k: f"{type(v).__name__}={repr(v)[:80]}" for k, v in cleaned.items()}
        log.severe(f"Failed to upsert into {table}: {e}. Fields: {problem_fields}")
        raise
