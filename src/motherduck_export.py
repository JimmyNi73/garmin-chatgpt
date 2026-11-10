"""Controlled record-level Garmin -> MotherDuck test.

Uses the same Garmin Personal API client and detail validation as garmin_export.py.
No Dropbox reads/writes, gear endpoint calls, month-partitioned database storage,
or destructive database operations.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from garmin_export import (
    GarminCLI, activity_records, date_index, device_registry,
    fetch_activities, fetch_month_health, fetch_gear_data, enrich_gear_records,
)
from motherduck_loader import connect, ensure_schema, load_records

LOG = logging.getLogger("motherduck_export")
TZ = ZoneInfo("Europe/Stockholm")
HISTORIC_MONTHS = ("2021-08", "2023-08")


def month_bounds(month: str) -> tuple[date, date]:
    first = date.fromisoformat(month + "-01")
    if first.month == 12:
        next_month = date(first.year + 1, 1, 1)
    else:
        next_month = date(first.year, first.month + 1, 1)
    return first, next_month - timedelta(days=1)


def date_of(row: dict) -> str | None:
    for key in ("calendarDate", "date", "measurementDate", "startTimeLocal",
                "startTimeGMT", "startDate", "summaryDate"):
        value = row.get(key)
        if value:
            return str(value)[:10]
    return None


def health_records(source: str, health: dict, allowed_start: date,
                   allowed_end: date) -> list[dict]:
    """Flatten date-indexed and daily health data without collapsing same-day samples."""
    out: list[dict] = []
    range_data = health.get("range_data", {})
    for metric, indexed in range_data.items():
        if not isinstance(indexed, dict):
            continue
        by_date = indexed.get("by_date", {})
        if isinstance(by_date, dict):
            for day, payload in by_date.items():
                try:
                    d = date.fromisoformat(str(day)[:10])
                except ValueError:
                    continue
                if allowed_start <= d <= allowed_end and isinstance(payload, dict):
                    out.append({"metric_source": metric, "calendarDate": d.isoformat(),
                                "source_payload": payload})
        raw = indexed.get("raw")
        if raw is not None and not by_date:
            # Retain unindexed source response as one complete RAW record.
            out.append({"metric_source": metric, "calendarDate": None,
                        "source_payload": raw})
    for day, payload in health.get("by_date", {}).items():
        try:
            d = date.fromisoformat(str(day)[:10])
        except ValueError:
            continue
        if not (allowed_start <= d <= allowed_end) or not isinstance(payload, dict):
            continue
        for metric, value in payload.items():
            if value is not None:
                out.append({"metric_source": metric, "calendarDate": d.isoformat(),
                            "source_payload": value})
    return out


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    if not os.getenv("GARMIN_PERSONAL_API_KEY") or not os.getenv("MOTHERDUCK_TOKEN"):
        raise RuntimeError("GARMIN_PERSONAL_API_KEY and MOTHERDUCK_TOKEN are required")

    today = datetime.now(TZ).date()
    recent_start = today - timedelta(days=13)
    run_id = str(uuid.uuid4())
    cli = GarminCLI()
    datasets: dict[str, list[dict]] = {}
    summaries_by_id: dict[str, dict] = {}

    windows = []
    for month in HISTORIC_MONTHS:
        start, end = month_bounds(month)
        windows.append((start, end, f"historic:{month}"))
    windows.append((recent_start, today, "recent:14d"))

    for start, end, label in windows:
        LOG.info("Fetching activities %s (%s..%s)", label, start, end)
        summaries = fetch_activities(cli, start.isoformat(), end.isoformat())
        for summary in summaries:
            aid = summary.get("activityId", summary.get("activity_id", summary.get("id")))
            if aid is None:
                raise RuntimeError(f"{label}: activity summary without activityId")
            summaries_by_id[str(aid)] = summary

        # Health API calls are month-bounded by Garmin's own API restrictions.
        cursor = start.replace(day=1)
        while cursor <= end:
            month = cursor.strftime("%Y-%m")
            health = fetch_month_health(cli, month, today=today)
            meta = health.get("metadata", {})
            if meta.get("export_status") != "complete":
                raise RuntimeError(f"Health source errors for {label}/{month}: "
                                   f"{health.get('source_errors', [])}")
            for row in health_records("health", health, start, end):
                metric = row.pop("metric_source")
                datasets.setdefault(metric, []).append(row)
            if cursor.month == 12:
                cursor = date(cursor.year + 1, 1, 1)
            else:
                cursor = date(cursor.year, cursor.month + 1, 1)

    summaries = list(summaries_by_id.values())
    activities = activity_records(cli, summaries) if summaries else []
    detail_ids = [str(row.get("summary", {}).get("activityId")) for row in activities]
    expected_ids = set(summaries_by_id)
    if len(detail_ids) != len(set(detail_ids)) or set(detail_ids) != expected_ids:
        raise RuntimeError("Activity detail preflight failed; refusing all database writes")
    # Fetch gear registry and activity assignments directly from Garmin Personal API.
    gear_registry, activity_to_gear = fetch_gear_data(cli)
    if not gear_registry:
        raise RuntimeError("Garmin returned an empty gear registry; refusing incomplete test")
    enrich_gear_records(activities, gear_registry, activity_to_gear)
    unknown_gear_ids = set(activity_to_gear.values()) - set(gear_registry)
    if unknown_gear_ids:
        raise RuntimeError(f"Garmin gear links reference unknown gear IDs: {sorted(unknown_gear_ids)}")

    datasets["activities"] = activities
    datasets["devices"] = device_registry(activities)
    datasets["gear_registry"] = [
        {"gear_id": gear_id, **gear} for gear_id, gear in sorted(gear_registry.items())
    ]

    # VO2 is loaded only when present in documented activity summaries.
    vo2 = []
    for summary in summaries:
        value = summary.get("vO2MaxValue")
        stamp = summary.get("startTimeLocal") or summary.get("startTimeGMT")
        if value is not None and stamp:
            vo2.append({"calendarDate": str(stamp)[:10],
                        "activityId": str(summary.get("activityId")),
                        "vo2MaxValue": value, "source_payload": summary})
    if vo2:
        datasets["vo2_max"] = vo2
    else:
        LOG.warning("No VO2 values returned in documented activity summaries")

    # De-duplicate exact health payloads while retaining distinct same-day samples.
    for source, rows in list(datasets.items()):
        unique = {}
        for row in rows:
            key = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
            unique[key] = row
        datasets[source] = list(unique.values())

    mapped_activities = sum(1 for row in activities if isinstance(row.get("gear"), dict)
                            and row["gear"].get("gear_id") is not None)
    weight_rows = len(datasets.get("weight_measurements", []))
    LOG.info("Preflight passed: %d activities, %d with gear, %d Garmin gear items, "
             "%d weight records, %d Garmin requests, sources=%s",
             len(activities), mapped_activities, len(gear_registry), weight_rows,
             cli.request_count, {name: len(rows) for name, rows in datasets.items()})

    # No writes occur before all Garmin retrieval and activity completeness checks pass.
    con = connect()
    try:
        ensure_schema(con)
        total = 0
        for source, rows in datasets.items():
            total += load_records(con, source, rows, run_id)
        model_path = Path(__file__).resolve().parents[1] / "sql" / "garmin_motherduck_model.sql"
        con.execute(model_path.read_text(encoding="utf-8"))
        counts = con.execute("""
            SELECT source_name, count(*) FROM raw.garmin_payloads
            GROUP BY source_name ORDER BY source_name
        """).fetchall()
        LOG.info("MotherDuck test succeeded: run=%s loaded=%d counts=%s", run_id, total, counts)
    finally:
        con.close()


if __name__ == "__main__":
    main()
