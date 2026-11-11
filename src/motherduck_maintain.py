"""Incremental Garmin -> MotherDuck maintenance (rolling 30-day overlap).

This entrypoint is intentionally incapable of full-history ingestion. It fetches
only the last 30 days, validates the complete payload before writing, and uses
the idempotent loader to upsert existing natural keys.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from garmin_export import (
    GarminCLI,
    activity_records,
    device_registry,
    enrich_gear_records,
    fetch_activities,
    fetch_gear_data,
    fetch_month_health,
)
from motherduck_export import health_records
from motherduck_loader import load_records, stable_record_key

LOG = logging.getLogger("motherduck_maintain")
TZ = ZoneInfo("Europe/Stockholm")
DATABASE = "garmin_github"
WINDOW_DAYS = 30
BATCH_SIZE = 100


def _activity_id(row: dict) -> str | None:
    summary = row.get("summary") if isinstance(row.get("summary"), dict) else row
    value = summary.get("activityId", summary.get("activity_id", summary.get("id")))
    return str(value) if value is not None else None


def _row_date(row: dict) -> date | None:
    summary = row.get("summary") if isinstance(row.get("summary"), dict) else row
    for key in ("startTimeLocal", "startTimeGMT", "calendarDate", "date"):
        value = summary.get(key)
        if value:
            try:
                return date.fromisoformat(str(value)[:10])
            except ValueError:
                return None
    return None


def _validate_activities(summaries: list[dict], details: list[dict],
                         start: date, end: date) -> None:
    ids = [_activity_id(row) for row in summaries]
    if not ids or None in ids or len(ids) != len(set(ids)):
        raise RuntimeError("Activity index is empty or contains missing/duplicate IDs")
    for row in summaries:
        day = _row_date(row)
        if day is None or not (start <= day <= end):
            raise RuntimeError(f"Activity index contains invalid/out-of-window row: {_activity_id(row)}")
    detail_ids = [_activity_id(row) for row in details]
    if (len(detail_ids) != len(ids) or None in detail_ids
            or len(detail_ids) != len(set(detail_ids)) or set(detail_ids) != set(ids)):
        raise RuntimeError("Activity detail preflight failed; refusing all writes")


def _validate_health(health: dict, start: date, end: date) -> list[dict]:
    meta = health.get("metadata", {})
    if meta.get("export_status") != "complete":
        raise RuntimeError(f"Garmin health retrieval incomplete: {health.get('source_errors', [])}")
    rows = health_records("health", health, start, end)
    for row in rows:
        raw_day = row.get("calendarDate")
        if raw_day is not None:
            try:
                day = date.fromisoformat(str(raw_day)[:10])
            except ValueError as exc:
                raise RuntimeError(f"Invalid health date: {raw_day}") from exc
            if not (start <= day <= end):
                raise RuntimeError(f"Health row outside rolling window: {raw_day}")
    return rows


def _group_health(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        source = row.get("metric_source")
        if not source:
            raise RuntimeError("Health row missing metric_source")
        grouped[str(source)].append({k: v for k, v in row.items() if k != "metric_source"})
    # Collapse exact duplicates only. Multiple weigh-ins remain separate.
    result = {}
    for source, items in grouped.items():
        unique = {json.dumps(item, sort_keys=True, ensure_ascii=False, default=str): item
                  for item in items}
        result[source] = list(unique.values())
    return result


def _existing_keys(con, source: str, keys: list[str]) -> set[str]:
    found: set[str] = set()
    for offset in range(0, len(keys), 400):
        batch = keys[offset:offset + 400]
        if not batch:
            continue
        placeholders = ",".join("?" for _ in batch)
        rows = con.execute(
            f"SELECT record_key FROM raw.garmin_payloads "
            f"WHERE source_name = ? AND record_key IN ({placeholders})",
            [source, *batch],
        ).fetchall()
        found.update(row[0] for row in rows)
    return found


def _dry_run_report(con, activities: list[dict], health_by_source: dict[str, list[dict]],
                    devices: list[dict], gear: list[dict], vo2: list[dict]) -> None:
    groups = {"activities": activities, **health_by_source}
    if devices:
        groups["devices"] = devices
    if gear:
        groups["gear_registry"] = gear
    if vo2:
        groups["vo2_max"] = vo2
    total_new = total_existing = 0
    LOG.info("DRY RUN — no MotherDuck writes will be performed")
    for source, rows in sorted(groups.items()):
        keys = [stable_record_key(source, row, i) for i, row in enumerate(rows)]
        existing = _existing_keys(con, source, keys)
        new_count = sum(1 for key in keys if key not in existing)
        existing_count = len(keys) - new_count
        total_new += new_count
        total_existing += existing_count
        LOG.info("DRY RUN source=%s fetched=%d new=%d existing/upsert=%d",
                 source, len(rows), new_count, existing_count)
    LOG.info("DRY RUN complete: fetched=%d new=%d existing/upsert=%d",
             sum(len(v) for v in groups.values()), total_new, total_existing)


def _load_batches(con, source: str, rows: list[dict], run_id: str) -> int:
    loaded = 0
    for offset in range(0, len(rows), BATCH_SIZE):
        loaded += load_records(con, source, rows[offset:offset + BATCH_SIZE], run_id)
        LOG.info("Loaded source=%s rows=%d/%d", source,
                 min(offset + BATCH_SIZE, len(rows)), len(rows))
    return loaded


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")

    # Hard safety gate: this script can never be switched into full-history mode.
    if os.getenv("GARMIN_HISTORY_MODE", "incremental").strip().lower() != "incremental":
        raise RuntimeError("This entrypoint only supports GARMIN_HISTORY_MODE=incremental")
    if not os.getenv("GARMIN_PERSONAL_API_KEY") or not os.getenv("MOTHERDUCK_TOKEN"):
        raise RuntimeError("GARMIN_PERSONAL_API_KEY and MOTHERDUCK_TOKEN are required")

    dry_run = os.getenv("GARMIN_MOTHERDUCK_DRY_RUN", "true").strip().lower() == "true"
    today = datetime.now(TZ).date()
    start = today - timedelta(days=WINDOW_DAYS - 1)
    run_id = str(uuid.uuid4())
    cli = GarminCLI()

    LOG.info("Run mode=%s window=%s..%s (%d days) database=%s",
             "DRY_RUN" if dry_run else "WRITE", start, today, WINDOW_DAYS, DATABASE)

    # Fetch and validate everything before opening a write-capable database session.
    summaries = fetch_activities(cli, start.isoformat(), today.isoformat())
    if not summaries:
        raise RuntimeError("Garmin returned no activities for the rolling window; refusing run")
    details = activity_records(cli, summaries)
    _validate_activities(summaries, details, start, today)

    health = fetch_month_health(cli, today.strftime("%Y-%m"), today=today)
    health_rows = _validate_health(health, start, today)
    health_by_source = _group_health(health_rows)

    try:
        gear_registry, activity_to_gear = fetch_gear_data(cli)
    except Exception as exc:
        LOG.warning("Gear registry unavailable; retaining existing gear values: %s", exc)
        gear_registry, activity_to_gear = {}, {}
    if gear_registry:
        enrich_gear_records(details, gear_registry, activity_to_gear)

    devices = device_registry(details)
    gear_rows = [{"gear_id": gear_id, **item}
                 for gear_id, item in sorted(gear_registry.items())]

    vo2 = []
    for summary in summaries:
        value = summary.get("vO2MaxValue")
        day = _row_date(summary)
        aid = _activity_id(summary)
        if value is not None and day is not None and aid is not None:
            vo2.append({"calendarDate": day.isoformat(), "activityId": aid,
                        "vo2MaxValue": value, "source_payload": summary})

    # Connect to the already existing database; do not create or alter it in dry-run.
    con = duckdb.connect(f"md:{DATABASE}")
    try:
        required = con.execute("""
            SELECT count(*) FROM information_schema.tables
            WHERE table_schema='raw' AND table_name='garmin_payloads'
        """).fetchone()[0]
        if required != 1:
            raise RuntimeError("Expected existing raw.garmin_payloads table; refusing schema bootstrap")
        _dry_run_report(con, details, health_by_source, devices, gear_rows, vo2)
        if dry_run:
            LOG.info("Dry-run finished successfully. No database writes or model changes.")
            return

        # Schema changes are intentionally not part of Run & Maintain.
        loaded = 0
        loaded += _load_batches(con, "activities", details, run_id)
        for source, rows in sorted(health_by_source.items()):
            loaded += _load_batches(con, source, rows, run_id)
        if devices:
            loaded += _load_batches(con, "devices", devices, run_id)
        if gear_rows:
            loaded += _load_batches(con, "gear_registry", gear_rows, run_id)
        if vo2:
            loaded += _load_batches(con, "vo2_max", vo2, run_id)

        model_path = Path(__file__).resolve().parents[1] / "sql" / "garmin_motherduck_model.sql"
        con.execute(model_path.read_text(encoding="utf-8"))
        counts = con.execute("""
            SELECT source_name, count(*) FROM raw.garmin_payloads
            GROUP BY source_name ORDER BY source_name
        """).fetchall()
        LOG.info("Run & Maintain succeeded: run=%s loaded=%d raw_counts=%s",
                 run_id, loaded, counts)
    finally:
        con.close()


if __name__ == "__main__":
    main()
