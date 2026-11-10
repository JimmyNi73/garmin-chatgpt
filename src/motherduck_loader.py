"""Idempotent RAW loader for complete Garmin monthly payloads in MotherDuck."""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Iterable

import duckdb


def stable_record_key(source_name: str, record: dict[str, Any], index: int) -> str:
    """Prefer stable Garmin identifiers, including nested activity summaries and date-stamped records."""
    candidates = [
        record.get("activityId"), record.get("activity_id"), record.get("id"),
        (record.get("summary") or {}).get("activityId")
        if isinstance(record.get("summary"), dict) else None,
    ]
    for value in candidates:
        if value is not None:
            return str(value)
    canonical = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(f"{source_name}:{canonical}".encode()).hexdigest()


def connect(database: str | None = None) -> duckdb.DuckDBPyConnection:
    if not os.getenv("MOTHERDUCK_TOKEN"):
        raise RuntimeError("MOTHERDUCK_TOKEN is not configured")
    target = database or os.getenv("MOTHERDUCK_DATABASE", "garmin_github")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", target):
        raise ValueError(f"Invalid MotherDuck database identifier: {target!r}")
    admin = duckdb.connect("md:")
    try:
        admin.execute(f"CREATE DATABASE IF NOT EXISTS {target}")
    finally:
        admin.close()
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
    for key in ("calendarDate", "date", "startTimeLocal", "startTimeGMT",
                "beginTimestamp", "startTimeGmt"):
        value = record.get(key)
        if value:
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(value / 1000, tz=timezone.utc).date().isoformat()
            return str(value)[:10]
    summary = record.get("summary")
    if isinstance(summary, dict):
        for key in ("calendarDate", "startTimeLocal", "startTimeGMT",
                    "beginTimestamp", "startTimeGmt"):
            value = summary.get(key)
            if value:
                if isinstance(value, (int, float)):
                    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).date().isoformat()
                return str(value)[:10]
    return None


def load_records(con: duckdb.DuckDBPyConnection, source_name: str,
                 records: Iterable[dict[str, Any]], source_run_id: str) -> int:
    rows = []
    loaded_at = datetime.now(timezone.utc)
    for index, record in enumerate(records):
        rows.append((source_name, stable_record_key(source_name, record, index),
                     event_date(record),
                     json.dumps(record, ensure_ascii=False, separators=(",", ":")),
                     source_run_id, loaded_at))
    if not rows:
        return 0
    con.execute("BEGIN TRANSACTION")
    try:
        con.executemany("""
            INSERT INTO raw.garmin_payloads
            VALUES (?, ?, TRY_CAST(? AS DATE), ?::JSON, ?, ?)
            ON CONFLICT (source_name, record_key) DO UPDATE SET
                event_date = excluded.event_date, payload = excluded.payload,
                source_run_id = excluded.source_run_id, loaded_at = excluded.loaded_at
        """, rows)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return len(rows)

