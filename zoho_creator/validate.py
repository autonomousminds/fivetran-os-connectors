"""
Local validation runner for the Zoho Creator connector.

The Fivetran SDK tester JAR ships an x86_64 JVM + DuckDB JNI library that
crash on Apple Silicon under Rosetta (SIGBUS in libjvm / SIGSEGV in
libduckdb_java). This script monkey-patches `Operations.upsert/checkpoint/
delete` to buffer records in Python, then writes them to a local DuckDB file
via the native ARM64 `duckdb` package — no JVM, no Rosetta, no crashes.

Same `update()` code path as production. If this script completes cleanly,
`fivetran deploy` should also work; the JVM crashes are a local-tester
artefact only.

Usage:
    conda activate fivetran-productive
    cd zoho_creator
    python validate.py
"""

import json
import os
import sys
from collections import defaultdict

import fivetran_connector_sdk  # noqa: F401
from fivetran_connector_sdk import Logging, Operations as op

# Verbose — per-page bulk-read progress and incremental checkpoint logs only
# fire at FINE, and without them this script looks frozen during a multi-form
# initial backfill.
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
    sys.exit("configuration.json not found — copy configuration.json.example and fill it in")

with open(config_path) as f:
    configuration = json.load(f)

REQUIRED = ("client_id", "client_secret", "refresh_token",
            "account_owner_name", "data_center")
missing = [k for k in REQUIRED if not configuration.get(k)]
if missing:
    sys.exit(f"configuration.json missing required key(s): {', '.join(missing)}")


# ── Resume from previous local run if present ───────────────────────────────
state: dict = {}
if os.path.exists(_state_path):
    with open(_state_path) as f:
        state = json.load(f)
    print(f"Resuming from local state: keys={list(state.keys())[:10]}{'...' if len(state) > 10 else ''}")


# ── Run the connector ───────────────────────────────────────────────────────
import connector  # noqa: E402

print("=" * 70)
print("Running connector.update() with local stubs (no JVM, no Rosetta)")
print("=" * 70)
connector.update(configuration, state)


# ── Persist buffered records to a native DuckDB file ────────────────────────
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

for table in sorted(_buffer.keys()):
    rows = _buffer[table]
    if not rows:
        print(f"  {table:60s}  0")
        continue

    # Build column union, case-insensitively (DuckDB rejects two columns
    # whose names differ only in case — match what real warehouses do).
    # If the connector emits two keys that lowercase to the same target,
    # take the value from whichever non-null occurrence we see first.
    columns: list = []
    seen_lc: dict = {}  # lowercase -> actual stored column key
    for r in rows:
        for k in r.keys():
            kl = k.lower()
            if kl not in seen_lc:
                seen_lc[kl] = kl
                columns.append(kl)

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
        # Collapse keys case-insensitively. Two source keys that lowercase
        # to the same target → take the first non-null/non-empty value seen.
        rl: dict = {}
        for k, v in r.items():
            kl = k.lower()
            if kl not in rl or rl[kl] in (None, ""):
                rl[kl] = v
        row = []
        for c in columns:
            v = rl.get(c)
            if v is None or isinstance(v, (bool, int, float, str)):
                row.append(v)
            else:
                row.append(str(v))
        payload.append(row)
    con.executemany(f'INSERT INTO "{table}" VALUES ({placeholders})', payload)
    n = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    print(f"  {table:60s}  {n}")

print()
print(f"Checkpoints emitted: {_checkpoint_count}")
print(f"Tables with deletes queued: {sum(1 for v in _delete_buffer.values() if v)}")
print(f"DuckDB file: {db_path}")
print()
print("Inspect with:")
print(f'  duckdb {db_path} -c "SHOW TABLES;"')
print(f'  duckdb {db_path} -c "SELECT COUNT(*) FROM applications;"')
print(f'  duckdb {db_path} -c "SELECT app_link_name, link_name FROM forms LIMIT 10;"')
con.close()
