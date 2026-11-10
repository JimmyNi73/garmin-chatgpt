"""Idempotent Garmin RAW + relational CORE loader for MotherDuck."""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Iterable

import duckdb


def _summary(record: dict[str, Any]) -> dict[str, Any]:
    value = record.get("summary")
    return value if isinstance(value, dict) else record


def _first(*values: Any) -> Any:
    return next((value for value in values if value is not None and value != ""), None)


def stable_record_key(source_name: str, record: dict[str, Any], index: int) -> str:
    """Use Garmin natural IDs before fallback hashes; nested activity IDs win."""
    summary = _summary(record)
    candidates = [
        summary.get("activityId"), record.get("activityId"), record.get("activity_id"),
        record.get("measurementId"), record.get("calendarDate") if source_name == "daily_summaries" else None,
        record.get("id"),
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
    con.execute("CREATE SCHEMA IF NOT EXISTS core")
    con.execute("CREATE SCHEMA IF NOT EXISTS mart")
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.garmin_payloads (
            source_name VARCHAR NOT NULL, record_key VARCHAR NOT NULL,
            event_date DATE, payload JSON NOT NULL,
            source_run_id VARCHAR NOT NULL, loaded_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (source_name, record_key)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS core.activities (
            activity_id VARCHAR PRIMARY KEY, activity_date DATE, activity_type VARCHAR,
            activity_name VARCHAR, distance_m DOUBLE, duration_s DOUBLE,
            elapsed_duration_s DOUBLE, average_speed_m_s DOUBLE,
            average_hr DOUBLE, max_hr DOUBLE, calories DOUBLE,
            average_run_cadence DOUBLE, max_run_cadence DOUBLE, stride_length_mm DOUBLE,
            aerobic_training_effect DOUBLE, activity_training_load DOUBLE, vo2_max DOUBLE,
            device_id VARCHAR, gear_id VARCHAR, source_payload JSON NOT NULL,
            source_run_id VARCHAR NOT NULL, loaded_at TIMESTAMPTZ NOT NULL
        )
    """)
    # CREATE TABLE IF NOT EXISTS does not evolve existing tables.
    for column, dtype in (
        ("stride_length_mm", "DOUBLE"),
        ("aerobic_training_effect", "DOUBLE"),
        ("activity_training_load", "DOUBLE"),
    ):
        con.execute(f"ALTER TABLE core.activities ADD COLUMN IF NOT EXISTS {column} {dtype}")
    con.execute("""
        CREATE TABLE IF NOT EXISTS core.activity_workouts (
            activity_id VARCHAR NOT NULL, workout_index INTEGER NOT NULL,
            workout_payload JSON NOT NULL, source_run_id VARCHAR NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (activity_id, workout_index)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS core.daily_metrics (
            metric_source VARCHAR NOT NULL, record_key VARCHAR NOT NULL,
            metric_date DATE, metric_value JSON, source_payload JSON NOT NULL,
            source_run_id VARCHAR NOT NULL, loaded_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (metric_source, record_key)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS core.devices (
            device_id VARCHAR PRIMARY KEY, device_model VARCHAR,
            source_payload JSON NOT NULL, source_run_id VARCHAR NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS core.gear (
            gear_id VARCHAR PRIMARY KEY, gear_name VARCHAR,
            source_payload JSON NOT NULL, source_run_id VARCHAR NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL
        )
    """)


def event_date(record: dict[str, Any]) -> str | None:
    summary = _summary(record)
    for key in ("calendarDate", "date", "measurementDate", "startTimeLocal",
                "startTimeGMT", "beginTimestamp", "startTimeGmt", "startDate",
                "summaryDate"):
        value = _first(record.get(key), summary.get(key))
        if value:
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(value / 1000, tz=timezone.utc).date().isoformat()
            return str(value)[:10]
    return None


def _as_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _bulk_insert(con: duckdb.DuckDBPyConnection, table: str, rows: list[tuple],
                 value_template: str, conflict_clause: str = "",
                 chunk_size: int = 500) -> None:
    """Send bounded multi-row INSERT statements instead of one network round-trip per row."""
    if not rows:
        return
    tuple_sql = f"({value_template})"
    for offset in range(0, len(rows), chunk_size):
        batch = rows[offset:offset + chunk_size]
        values_sql = ", ".join([tuple_sql] * len(batch))
        params = [value for row in batch for value in row]
        con.execute(
            f"INSERT INTO {table} VALUES {values_sql} {conflict_clause}",
            params,
        )


def load_records(con: duckdb.DuckDBPyConnection, source_name: str,
                 records: Iterable[dict[str, Any]], source_run_id: str) -> int:
    rows = list(records)
    if not rows:
        return 0
    loaded_at = datetime.now(timezone.utc)
    raw_rows = []
    for index, record in enumerate(rows):
        raw_rows.append((source_name, stable_record_key(source_name, record, index),
                         event_date(record), _json(record), source_run_id, loaded_at))

    con.execute("BEGIN TRANSACTION")
    try:
        _bulk_insert(con, "raw.garmin_payloads", raw_rows,
            "?, ?, TRY_CAST(? AS DATE), ?::JSON, ?, ?",
            """ON CONFLICT (source_name, record_key) DO UPDATE SET
                event_date=excluded.event_date, payload=excluded.payload,
                source_run_id=excluded.source_run_id, loaded_at=excluded.loaded_at""")

        if source_name == "activities":
            # Collapse legacy hash-keyed copies to the Garmin activityId key.
            # CORE is naturally unique by activity_id.
            con.execute("""
                DELETE FROM raw.garmin_payloads
                WHERE source_name = 'activities'
                  AND record_key <> coalesce(
                      json_extract_string(payload, '$.summary.activityId'),
                      json_extract_string(payload, '$.activityId'),
                      record_key
                  )
            """)
            core_rows = []
            for index, record in enumerate(rows):
                s = _summary(record)
                activity_id = _first(s.get("activityId"), record.get("activityId"),
                                      record.get("activity_id"), record.get("id"))
                if activity_id is None:
                    raise ValueError("Activity record missing Garmin activityId")
                activity_type = s.get("activityType")
                if isinstance(activity_type, dict):
                    activity_type = _first(activity_type.get("typeKey"), activity_type.get("typeName"))
                gear = record.get("gear")
                gear_data = gear.get("gear") if isinstance(gear, dict) else None
                gear_id = _first(
                    gear.get("gear_id") if isinstance(gear, dict) else None,
                    gear_data.get("gearPk") if isinstance(gear_data, dict) else None,
                    gear_data.get("gearId") if isinstance(gear_data, dict) else None,
                    s.get("gearId"))
                detail = record.get("detail") if isinstance(record.get("detail"), dict) else {}
                detail_summary = detail.get("summaryDTO") if isinstance(detail.get("summaryDTO"), dict) else {}
                # Garmin API versions expose equivalent measures under different names.
                def metric(*keys):
                    for obj in (s, detail_summary, detail):
                        value = _first(*(obj.get(key) for key in keys))
                        if value is not None:
                            return _as_float(value)
                    return None
                device_id = _first(
                    s.get("deviceId"), detail.get("deviceId"),
                    (record.get("device") or {}).get("device_id")
                    if isinstance(record.get("device"), dict) else None)
                core_rows.append((
                    str(activity_id), event_date(record), activity_type,
                    _first(s.get("activityName"), record.get("activityName")),
                    _as_float(s.get("distance")), _as_float(s.get("duration")),
                    _as_float(s.get("elapsedDuration")), _as_float(s.get("averageSpeed")),
                    _as_float(s.get("averageHR")), _as_float(s.get("maxHR")),
                    _as_float(s.get("calories")),
                    metric("averageRunCadence", "averageRunningCadenceInStepsPerMinute", "avgRunCadence"),
                    metric("maxRunCadence", "maxRunningCadenceInStepsPerMinute"),
                    metric("avgStrideLength", "strideLength", "averageStrideLength"),
                    metric("aerobicTrainingEffect", "trainingEffect"),
                    metric("activityTrainingLoad", "trainingLoad"),
                    metric("vO2MaxValue", "vo2Max", "vo2MaxValue"),
                    str(device_id) if device_id is not None else None,
                    str(gear_id) if gear_id is not None else None,
                    _json(record), source_run_id, loaded_at))
            _bulk_insert(con, "core.activities", core_rows,
                "?,TRY_CAST(? AS DATE),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?::JSON,?,?",
                """ON CONFLICT (activity_id) DO UPDATE SET
                    activity_date=excluded.activity_date, activity_type=excluded.activity_type,
                    activity_name=excluded.activity_name, distance_m=excluded.distance_m,
                    duration_s=excluded.duration_s, elapsed_duration_s=excluded.elapsed_duration_s,
                    average_speed_m_s=excluded.average_speed_m_s, average_hr=excluded.average_hr,
                    max_hr=excluded.max_hr, calories=excluded.calories,
                    average_run_cadence=excluded.average_run_cadence,
                    max_run_cadence=excluded.max_run_cadence,
                    stride_length_mm=excluded.stride_length_mm,
                    aerobic_training_effect=excluded.aerobic_training_effect,
                    activity_training_load=excluded.activity_training_load,
                    vo2_max=excluded.vo2_max,
                    device_id=coalesce(excluded.device_id, core.activities.device_id),
                    gear_id=coalesce(excluded.gear_id, core.activities.gear_id),
                    source_payload=excluded.source_payload, source_run_id=excluded.source_run_id,
                    loaded_at=excluded.loaded_at""")
            workout_rows = []
            for record in rows:
                summary = _summary(record)
                activity_id = _first(summary.get("activityId"), record.get("activityId"),
                                      record.get("activity_id"), record.get("id"))
                workouts = record.get("workouts")
                if activity_id is not None and isinstance(workouts, list):
                    for workout_index, workout in enumerate(workouts):
                        workout_rows.append((str(activity_id), workout_index, _json(workout),
                                             source_run_id, loaded_at))
            if workout_rows:
                _bulk_insert(con, "core.activity_workouts", workout_rows,
                    "?, ?, ?::JSON, ?, ?",
                    """ON CONFLICT (activity_id, workout_index) DO UPDATE SET
                        workout_payload=excluded.workout_payload,
                        source_run_id=excluded.source_run_id, loaded_at=excluded.loaded_at""")
        elif source_name == "devices":
            device_rows = []
            for record in rows:
                device_id = _first(record.get("deviceId"), record.get("device_id"), record.get("id"))
                if device_id is None:
                    continue
                device_rows.append((str(device_id), _first(record.get("device_model"),
                    record.get("deviceModel"), record.get("model")), _json(record), source_run_id, loaded_at))
            if device_rows:
                _bulk_insert(con, "core.devices", device_rows, "?, ?, ?::JSON, ?, ?",
                    """ON CONFLICT (device_id) DO UPDATE SET device_model=excluded.device_model,
                    source_payload=excluded.source_payload, source_run_id=excluded.source_run_id,
                    loaded_at=excluded.loaded_at""")
        elif source_name in ("gear_registry", "gear"):
            gear_rows = []
            for record in rows:
                gear_id = _first(record.get("gearPk"), record.get("gearId"), record.get("gear_id"), record.get("id"))
                if gear_id is None:
                    continue
                gear_rows.append((str(gear_id), _first(record.get("customMakeModel"),
                    record.get("name"), record.get("displayName")), _json(record), source_run_id, loaded_at))
            if gear_rows:
                _bulk_insert(con, "core.gear", gear_rows, "?, ?, ?::JSON, ?, ?",
                    """ON CONFLICT (gear_id) DO UPDATE SET gear_name=excluded.gear_name,
                    source_payload=excluded.source_payload, source_run_id=excluded.source_run_id,
                    loaded_at=excluded.loaded_at""")
        else:
            metric_rows = []
            for index, record in enumerate(rows):
                key = stable_record_key(source_name, record, index)
                value = record.get("source_payload", record)
                metric_rows.append((source_name, key, event_date(record), _json(value),
                                    _json(record), source_run_id, loaded_at))
            _bulk_insert(con, "core.daily_metrics", metric_rows,
                "?, ?, TRY_CAST(? AS DATE), ?::JSON, ?::JSON, ?, ?",
                """ON CONFLICT (metric_source, record_key) DO UPDATE SET
                    metric_date=excluded.metric_date, metric_value=excluded.metric_value,
                    source_payload=excluded.source_payload, source_run_id=excluded.source_run_id,
                    loaded_at=excluded.loaded_at""")

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return len(rows)
