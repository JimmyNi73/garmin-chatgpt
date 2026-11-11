#!/usr/bin/env python3
"""Destructive, bounded Garmin API -> MotherDuck validation load.

The source is Garmin Connect only. Dropbox/JSON exports are never read.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
from garmin_export import (
    GarminCLI, activity_records, device_registry, enrich_gear_records,
    fetch_activities, fetch_month_health, fetch_gear_data, optional,
)
from motherduck_export import health_records, load_in_batches
from motherduck_loader import connect, ensure_schema

LOG = logging.getLogger("motherduck_sample_test")
TZ = ZoneInfo("Europe/Stockholm")
BATCH = 100
WINDOWS = (("2011-06-01", "2011-06-30"), ("2022-08-01", "2022-08-31"))


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    if not os.getenv("GARMIN_PERSONAL_API_KEY") or not os.getenv("MOTHERDUCK_TOKEN"):
        raise RuntimeError("GARMIN_PERSONAL_API_KEY and MOTHERDUCK_TOKEN are required")
    if os.getenv("MOTHERDUCK_DATABASE", "garmin_github") != "garmin_github":
        raise RuntimeError("This approved test is scoped to MotherDuck database garmin_github")

    today = datetime.now(TZ).date()
    recent_start = today - timedelta(days=13)
    windows = [(date.fromisoformat(a), date.fromisoformat(b)) for a, b in WINDOWS]
    windows.append((recent_start, today))
    run_id = str(uuid.uuid4())
    cli = GarminCLI()

    # Garmin API acquisition and complete preflight happen before destructive SQL.
    summaries = {}
    for start, end in windows:
        for row in fetch_activities(cli, start.isoformat(), end.isoformat()):
            aid = row.get("activityId", row.get("activity_id", row.get("id")))
            if aid is None:
                raise RuntimeError(f"Garmin summary missing activityId in {start}..{end}")
            summaries[str(aid)] = row
    if not summaries:
        raise RuntimeError("Garmin returned zero activities across all three test windows")

    gear_registry, activity_to_gear = fetch_gear_data(cli)
    global_rows = {}
    errors = []
    for source, command in (("profile", "profile-settings"),
                            ("heart_rate_zones", "heart-rate-zones")):
        ok, value = optional(cli, errors, command, command)
        if not ok:
            raise RuntimeError(f"Garmin global source {command} failed: {errors}")
        if value is not None:
            global_rows[source] = [{"calendarDate": None, "source_payload": value}]

    health_by_source = {}
    month_windows = {}
    for start, end in windows:
        cursor = start.replace(day=1)
        while cursor <= end:
            month = cursor.strftime("%Y-%m")
            next_month = date(cursor.year + (cursor.month == 12), cursor.month % 12 + 1, 1)\n            month_end = next_month - timedelta(days=1)\n            month_windows.setdefault(month, []).append((max(start, cursor), min(end, month_end)))
            cursor = (date(cursor.year + (cursor.month == 12), cursor.month % 12 + 1, 1))
    for month in sorted(month_windows):
        health = fetch_month_health(cli, month, today=today, full_month=True)
        if health.get("metadata", {}).get("export_status") != "complete":
            raise RuntimeError(f"Garmin health errors for {month}: {health.get('source_errors')}")
        for start, end in month_windows[month]:
            for row in health_records("health", health, start, end):
                source = row.pop("metric_source")
                health_by_source.setdefault(source, []).append(row)

    activity_rows = []
    summary_list = list(summaries.values())
    for offset in range(0, len(summary_list), BATCH):
        batch = summary_list[offset:offset + BATCH]
        detailed = activity_records(cli, batch)
        expected = {str(x.get("activityId", x.get("activity_id", x.get("id")))) for x in batch}
        actual = [str(x["summary"].get("activityId", x["summary"].get("activity_id", x["summary"].get("id")))) for x in detailed]
        if len(actual) != len(expected) or len(actual) != len(set(actual)) or set(actual) != expected:
            raise RuntimeError(f"Activity detail preflight mismatch at offset {offset}")
        enrich_gear_records(detailed, gear_registry, activity_to_gear)
        activity_rows.extend(detailed)

    devices = device_registry(activity_rows)
    gear_rows = [{"gear_id": key, **value} for key, value in sorted(gear_registry.items())]
    vo2 = []
    for row in summary_list:
        value = row.get("vO2MaxValue")
        stamp = row.get("startTimeLocal") or row.get("startTimeGMT")
        if value is not None and stamp:
            vo2.append({"calendarDate": str(stamp)[:10], "activityId": str(row.get("activityId")),
                        "vo2MaxValue": value, "source_payload": row})

    # All source acquisition is complete. Only now truncate the six physical tables.
    con = connect("garmin_github")
    try:
        ensure_schema(con)
        con.execute("BEGIN TRANSACTION")
        try:
            for table in ("raw.garmin_payloads", "core.activities", "core.activity_workouts",
                          "core.daily_metrics", "core.devices", "core.gear"):
                con.execute(f"TRUNCATE TABLE {table}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        for source, rows in global_rows.items():
            load_in_batches(con, source, rows, run_id, BATCH)
        for source, rows in sorted(health_by_source.items()):
            # Preserve separate same-day measurements; remove only exact duplicates.
            unique = {canonical(row): row for row in rows}
            load_in_batches(con, source, list(unique.values()), run_id, BATCH)
        load_in_batches(con, "activities", activity_rows, run_id, BATCH)
        if devices:
            load_in_batches(con, "devices", devices, run_id, BATCH)
        if gear_rows:
            load_in_batches(con, "gear_registry", gear_rows, run_id, BATCH)
        if vo2:
            load_in_batches(con, "vo2_max", vo2, run_id, BATCH)

        model = Path(__file__).resolve().parents[1] / "sql" / "garmin_motherduck_model.sql"
        con.execute(model.read_text(encoding="utf-8"))

        # Deep equality of every stored activity attribute, including summary/detail/workouts.
        expected_activity = {str(x["summary"].get("activityId", x["summary"].get("activity_id", x["summary"].get("id")))): canonical(x)
                             for x in activity_rows}
        stored_activity = {str(aid): canonical(json.loads(payload)) for aid, payload in
                           con.execute("SELECT activity_id, source_payload FROM core.activities").fetchall()}
        raw_activity = {str(key): canonical(json.loads(payload)) for key, payload in
                        con.execute("SELECT record_key, payload FROM raw.garmin_payloads WHERE source_name='activities'").fetchall()}
        if expected_activity != stored_activity or expected_activity != raw_activity:
            missing = sorted(set(expected_activity) - set(stored_activity))
            extra = sorted(set(stored_activity) - set(expected_activity))
            changed = sorted(k for k in set(expected_activity) & set(stored_activity)
                              if expected_activity[k] != stored_activity[k])
            raise RuntimeError(f"Deep activity validation failed: missing={missing[:10]}, extra={extra[:10]}, changed={changed[:10]}")

        # Compare every expected health source row to both RAW and CORE JSON payloads.
        for source, rows in health_by_source.items():
            expected = {canonical(r) for r in {canonical(x): x for x in rows}.values()}
            raw = {canonical(json.loads(x[0])) for x in con.execute(
                "SELECT payload FROM raw.garmin_payloads WHERE source_name=?", [source]).fetchall()}
            core = {canonical(json.loads(x[0])) for x in con.execute(
                "SELECT source_payload FROM core.daily_metrics WHERE metric_source=?", [source]).fetchall()}
            if expected != raw or expected != core:
                raise RuntimeError(f"Full health payload validation failed for {source}: expected={len(expected)} raw={len(raw)} core={len(core)}")

        # Validate every workout, device, gear and global metadata payload too.
        expected_workouts = {
            (str(x["summary"].get("activityId", x["summary"].get("activity_id", x["summary"].get("id")))),
             index, canonical(workout))
            for x in activity_rows for index, workout in enumerate(x.get("workouts") or [])
        }
        actual_workouts = {
            (str(aid), int(idx), canonical(json.loads(payload)))
            for aid, idx, payload in con.execute(
                "SELECT activity_id, workout_index, workout_payload FROM core.activity_workouts").fetchall()
        }
        if expected_workouts != actual_workouts:
            raise RuntimeError(f"Workout validation failed: expected={len(expected_workouts)} actual={len(actual_workouts)}")

        expected_devices = {str(x["device_id"]): canonical(x) for x in devices}
        actual_devices = {str(aid): canonical(json.loads(payload)) for aid, payload in
                          con.execute("SELECT device_id, source_payload FROM core.devices").fetchall()}
        if expected_devices != actual_devices:
            raise RuntimeError("Device registry validation failed")
        expected_gear = {str(x["gear_id"]): canonical(x) for x in gear_rows}
        actual_gear = {str(aid): canonical(json.loads(payload)) for aid, payload in
                       con.execute("SELECT gear_id, source_payload FROM core.gear").fetchall()}
        if expected_gear != actual_gear:
            raise RuntimeError("Gear registry validation failed")
        for source, rows in global_rows.items():
            expected = {canonical(x) for x in rows}
            actual = {canonical(json.loads(x[0])) for x in con.execute(
                "SELECT payload FROM raw.garmin_payloads WHERE source_name=?", [source]).fetchall()}
            if expected != actual:
                raise RuntimeError(f"Global Garmin source validation failed: {source}")
        if vo2:
            expected_vo2 = {canonical(x) for x in vo2}
            actual_vo2 = {canonical(json.loads(x[0])) for x in con.execute(
                "SELECT payload FROM raw.garmin_payloads WHERE source_name='vo2_max'").fetchall()}
            if expected_vo2 != actual_vo2:
                raise RuntimeError("VO2 source payload validation failed")

        counts = con.execute("""
            SELECT 'raw.garmin_payloads', count(*) FROM raw.garmin_payloads
            UNION ALL SELECT 'core.activities', count(*) FROM core.activities
            UNION ALL SELECT 'core.activity_workouts', count(*) FROM core.activity_workouts
            UNION ALL SELECT 'core.daily_metrics', count(*) FROM core.daily_metrics
            UNION ALL SELECT 'core.devices', count(*) FROM core.devices
            UNION ALL SELECT 'core.gear', count(*) FROM core.gear
        """).fetchall()
        LOG.info("VALIDATION PASSED run_id=%s windows=%s activities=%d health_sources=%d counts=%s Garmin_requests=%d",
                 run_id, [(a.isoformat(), b.isoformat()) for a, b in windows],
                 len(activity_rows), len(health_by_source), counts, cli.request_count)
    finally:
        con.close()


if __name__ == "__main__":
    main()

