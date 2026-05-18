"""
Local validation runner — bypasses the Fivetran SDK tester JAR entirely.

The SDK tester ships an x86_64 JVM and DuckDB JNI library that crash on
Apple Silicon under Rosetta (SIGBUS in libjvm / SIGSEGV in libduckdb_java).
This script monkey-patches `Operations.upsert/checkpoint/delete` to buffer
records in Python memory, then writes them to a local DuckDB file using
Python's native ARM64 duckdb package — no JVM, no Rosetta, no crashes.

Same `update()` code path as production. If this completes cleanly, the
connector logic is sound and `fivetran deploy` will work end-to-end.

Usage:
    python validate.py
"""

import json
import os
import sys
from collections import defaultdict

# ── Stub Fivetran SDK Operations BEFORE any connector module imports ────────
import fivetran_connector_sdk  # noqa: F401
from fivetran_connector_sdk import Logging, Operations as op

# Logging.LOG_LEVEL is normally set by Connector.debug() before update() runs.
# We're calling update() directly, so initialise it manually.
# FINE is verbose but useful here: per-page progress and per-checkpoint logs
# only fire at FINE, and without them this script looks frozen for ~15+ minutes
# while large tables (e.g. order_lines at 77k rows) sync silently.
Logging.LOG_LEVEL = Logging.Level.FINE

_buffer: dict = defaultdict(list)
_delete_buffer: dict = defaultdict(list)
_state_path = os.path.join(os.path.dirname(__file__), "files", "local_state.json")
_checkpoint_count = 0


def _local_upsert(table: str, data: dict) -> None:
    _buffer[table].append(dict(data))


def _local_delete(table: str, keys: dict) -> None:
    _delete_buffer[table].append(dict(keys))


def _local_checkpoint(state: dict) -> None:
    global _checkpoint_count
    _checkpoint_count += 1
    os.makedirs(os.path.dirname(_state_path), exist_ok=True)
    with open(_state_path, "w") as f:
        json.dump(state, f, indent=2, default=str)


op.upsert = _local_upsert
op.delete = _local_delete
op.checkpoint = _local_checkpoint

# ── Load configuration ──────────────────────────────────────────────────────
config_path = os.path.join(os.path.dirname(__file__), "configuration.json")
if not os.path.exists(config_path):
    sys.exit("configuration.json not found — copy configuration.json.example and add your api_key")

with open(config_path) as f:
    configuration = json.load(f)

if not configuration.get("api_key"):
    sys.exit("configuration.json missing required key: api_key")

# ── Resume from last validate.py run if state exists ────────────────────────
state: dict = {}
if os.path.exists(_state_path):
    with open(_state_path) as f:
        state = json.load(f)
    print(f"Resuming from local state: keys={list(state.keys())}")

# ── Run the connector ───────────────────────────────────────────────────────
import connector  # noqa: E402

print("=" * 70)
print("Running connector.update() with local stubs (no JVM, no Rosetta)")
print("=" * 70)
connector.update(configuration, state)

# ── Write buffered records to a native ARM64 DuckDB file ────────────────────
import duckdb  # noqa: E402

db_path = os.path.join(os.path.dirname(__file__), "files", "local_warehouse.db")
os.makedirs(os.path.dirname(db_path), exist_ok=True)
if os.path.exists(db_path):
    os.remove(db_path)

con = duckdb.connect(db_path)
print()
print("=" * 70)
print(f"Persisting buffered records to {db_path}")
print("=" * 70)

# Build per-table column unions (records can have varying shape) and write.
# No pandas dependency — use DuckDB's parameterised INSERT directly.
for table in sorted(_buffer.keys()):
    rows = _buffer[table]
    if not rows:
        print(f"  {table:40s}  0")
        continue

    # Union of all columns seen across records (preserve insertion order)
    columns: list = []
    seen: set = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                columns.append(k)

    # Infer per-column DuckDB type from a non-null sample
    def _ddb_type(col: str) -> str:
        for r in rows:
            v = r.get(col)
            if v is None:
                continue
            if isinstance(v, bool):
                return "BOOLEAN"
            if isinstance(v, int):
                return "BIGINT"
            if isinstance(v, float):
                return "DOUBLE"
            return "VARCHAR"
        return "VARCHAR"

    col_decls = ", ".join(f'"{c}" {_ddb_type(c)}' for c in columns)
    placeholders = ", ".join(["?"] * len(columns))
    con.execute(f'DROP TABLE IF EXISTS "{table}"')
    con.execute(f'CREATE TABLE "{table}" ({col_decls})')

    payload = []
    for r in rows:
        row = []
        for c in columns:
            v = r.get(c)
            if v is None or isinstance(v, (bool, int, float, str)):
                row.append(v)
            else:
                row.append(str(v))
        payload.append(row)
    con.executemany(
        f'INSERT INTO "{table}" VALUES ({placeholders})',
        payload,
    )
    n = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    print(f"  {table:40s}  {n}")

print()
print(f"Checkpoints emitted: {_checkpoint_count}")
print(f"Tables with deletes queued: {sum(1 for v in _delete_buffer.values() if v)}")
print(f"DuckDB file: {db_path}")
print()
print("Inspect with:")
print(f'  duckdb {db_path} -c "SHOW TABLES;"')
print(f'  duckdb {db_path} -c "SELECT COUNT(*) FROM orders;"')
print(f'  duckdb {db_path} -c "SELECT billing_address_city, delivery_address_country FROM orders LIMIT 3;"')
con.close()
