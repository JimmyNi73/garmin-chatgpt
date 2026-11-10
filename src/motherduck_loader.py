"""Small, idempotent RAW loader for Garmin JSON payloads into MotherDuck.

The module deliberately separates extraction from loading. Extraction is supplied
by the pilot runner; this module never reads Dropbox archives or historical files.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Iterable

import duckdb


def stable_record_key(source_name: str, record: dict[str, Any], index: int) -> str:
    """Prefer stable Garmin identifiers; otherwise hash canonical JSON."""
    for key in ("activityId", "activity_id", "id", "calendarDate", "date"):
        value = record.get(key)
        if value is not None:
            return str(value)
    canonical = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(f"{source_name}:{canonical}:{index}".encode()).hexdigest()


def connect(database: str | None = None) -> duckdb.DuckDBPyConnection:
    if not os.getenv("MOTHERDUCK_TOKEN"):
        raise RuntimeError("MOTHERDUCK_TOKEN is not configured")
    target = database or os.getenv("MOTHERDUCK_DATABASE", "my_db")
    return duckdb.connect(f"md:{target}")


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.garmin_payloads (
            source_name VARCHAR NOT NULL,
            record_key VARCHAR NOT NULL,
            event_date DATE,
            payload JSON NOT NULL,
            source_run_id VARCHAR NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (source_name, record_key)
        )
    """)


def event_date(record: dict[str, Any]) -> str | None:
    for key in ("calendarDate", "date", "startTimeLocal", "startTimeGMT"):
        value = record.get(key)
        if value:
            return str(value)[:10]
    return None


def load_records(
    con: duckdb.DuckDBPyConnection,
    source_name: str,
    records: Iterable[dict[str, Any]],
    source_run_id: str,
) -> int:
    """Transactional upsert; re-running the same window cannot create duplicates."""
    rows = []
    loaded_at = datetime.now(timezone.utc)
    for index, record in enumerate(records):
        key = stable_record_key(source_name, record, index)
        rows.append((
            source_name, key, event_date(record),
            json.dumps(record, ensure_ascii=False, separators=(",", ":")),
            source_run_id, loaded_at,
        ))
    if not rows:
        return 0
    con.execute("BEGIN TRANSACTION")
    try:
        con.executemany("""
            INSERT INTO raw.garmin_payloads
            VALUES (?, ?, TRY_CAST(? AS DATE), ?::JSON, ?, ?)
            ON CONFLICT (source_name, record_key) DO UPDATE SET
                event_date = excluded.event_date,
                payload = excluded.payload,
                source_run_id = excluded.source_run_id,
                loaded_at = excluded.loaded_at
        """, rows)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return len(rows)
