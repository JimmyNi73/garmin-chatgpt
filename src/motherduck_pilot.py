"""14-day Garmin -> MotherDuck pilot. Reads live Garmin APIs only; no Dropbox or ZIP input."""
from __future__ import annotations

import logging
import os
import uuid
import requests
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from garmin_personal_api.api_keys import resolve_api_key
from garmin_personal_api.auth import PersonalApiAuthenticator
from garmin_personal_api.client import GarminPersonalApiClient

from garmin_export import (
    GarminCLI, activity_records, enrich_gear_records, fetch_activities,
    fetch_gear_data, optional_range, date_index,
)
from motherduck_loader import connect, ensure_schema, load_records

LOG = logging.getLogger("motherduck_pilot")
TZ = ZoneInfo("Europe/Stockholm")

RANGE_SOURCES = {
    "steps": "steps",
    "stress": "stress",
    "weight_measurements": "weight-measurements",
    "sleep_summaries": "sleep-summaries",
    "body_battery": "body-battery",
    "daily_summaries": "daily-summaries",
    "hrv_summaries": "hrv-summaries",
    "wellness_activity_summaries": "wellness-activity-summaries",
}


def records_from_indexed(value):
    """Flatten Garmin range responses, including nested weight summaries."""
    output = []
    def visit(node):
        if isinstance(node, list):
            for item in node:
                visit(item)
            return
        if not isinstance(node, dict):
            return
        for key in ("dailyWeightSummaries", "dailySummaries", "summaries", "items", "data"):
            nested = node.get(key)
            if isinstance(nested, list):
                for item in nested:
                    visit(item)
                return
        day = next((str(node[k])[:10] for k in
                    ("calendarDate", "date", "measurementDate", "startDate", "summaryDate")
                    if node.get(k)), None)
        if day:
            output.append({"calendarDate": day, **node})
            return
        for key, item in node.items():
            if isinstance(item, (dict, list)):
                visit(item)
    visit(value)
    # De-duplicate by date + measurement identifier/value; preserve multiple
    # weigh-ins on the same date.
    unique = {}
    for row in output:
        identity = (row["calendarDate"], row.get("samplePk") or row.get("measurementId")
                    or row.get("version") or repr(sorted(row.items())))
        unique[identity] = row
    return list(unique.values())


def fetch_range_chunked(cli, command, start, end, max_days=28):
    """Fetch Garmin date ranges in bounded windows; skip empty 400 windows."""
    merged = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=max_days - 1))
        try:
            value = cli.run(f"{command} {cursor}..{chunk_end}", command,
                            "--start-date", cursor.isoformat(),
                            "--end-date", chunk_end.isoformat())
            if isinstance(value, list):
                merged.extend(value)
            elif isinstance(value, dict):
                merged.append(value)
        except Exception as exc:
            if "400" in str(exc):
                LOG.warning("%s %s..%s: Garmin returned 400; no rows for this chunk",
                            command, cursor, chunk_end)
            else:
                raise
        cursor = chunk_end + timedelta(days=1)
    return merged


def normalize_vo2_response(value, requested_day=None):
    items = value if isinstance(value, list) else [value] if isinstance(value, dict) else []
    output = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("vo2MaxValue") is not None:
            day = item.get("calendarDate") or requested_day
            if day:
                output.append({"calendarDate": str(day)[:10],
                               "sport": item.get("sport") or item.get("maxMetCategory") or "unknown",
                               "vo2MaxValue": item["vo2MaxValue"], "source_payload": item})
            continue
        for sport in ("generic", "cycling"):
            metric = item.get(sport)
            if isinstance(metric, dict) and metric.get("vo2MaxValue") is not None:
                day = metric.get("calendarDate") or item.get("calendarDate") or requested_day
                if day:
                    output.append({"calendarDate": str(day)[:10], "sport": sport,
                                   "vo2MaxValue": metric["vo2MaxValue"],
                                   "source_payload": metric})
    return output


def vo2_rows_from_summaries(summaries):
    """Extract Garmin VO2 max values from documented activity summaries."""
    rows = []
    for summary in summaries:
        if not isinstance(summary, dict):
            continue
        value = summary.get("vO2MaxValue")
        if value is None:
            continue
        start = summary.get("startTimeLocal") or summary.get("startTimeGMT")
        if not start:
            continue
        activity_type = summary.get("activityType")
        if isinstance(activity_type, dict):
            sport = (activity_type.get("typeKey") or activity_type.get("typeName")
                     or activity_type.get("parentTypeId") or "unknown")
        else:
            sport = activity_type or "unknown"
        rows.append({
            "calendarDate": str(start)[:10],
            "activityId": str(summary.get("activityId") or ""),
            "sport": str(sport),
            "vo2MaxValue": value,
            "source_payload": summary,
        })
    return rows


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not os.getenv("MOTHERDUCK_TOKEN"):
        raise RuntimeError("GitHub secret MOTHERDUCK_TOKEN is missing; refusing to call Garmin before destination is ready")
    if not os.getenv("GARMIN_PERSONAL_API_KEY"):
        raise RuntimeError("GARMIN_PERSONAL_API_KEY is missing")
    start_arg = os.getenv("PILOT_START_DATE")
    end_arg = os.getenv("PILOT_END_DATE")
    if bool(start_arg) != bool(end_arg):
        raise ValueError("PILOT_START_DATE and PILOT_END_DATE must be supplied together")
    if start_arg and end_arg:
        start = date.fromisoformat(start_arg)
        end = date.fromisoformat(end_arg)
        if end < start:
            raise ValueError("PILOT_END_DATE must be on or after PILOT_START_DATE")
        days = (end - start).days + 1
    else:
        days = int(os.getenv("PILOT_DAYS", "14"))
        if days not in (7, 14, 30):
            raise ValueError("PILOT_DAYS must be 7, 14, or 30")
        end = datetime.now(TZ).date()
        start = end - timedelta(days=days - 1)
    start_text, end_text = start.isoformat(), end.isoformat()
    run_id = str(uuid.uuid4())
    LOG.info("Pilot window: %s through %s (%d days)", start_text, end_text, days)

    cli = GarminCLI()
    summaries = fetch_activities(cli, start_text, end_text)
    activities = activity_records(cli, summaries)
    datasets = {"activities": activities}
    errors = []

    # VO2 max is exposed by Garmin's documented activity-list endpoint
    # as SummarizedActivityDTO.vO2MaxValue. Do not call internal metrics-service URLs.
    vo2_rows = vo2_rows_from_summaries(summaries)
    datasets["vo2_max_daily"] = vo2_rows
    LOG.info("VO2 max from activity summaries: %d measurements", len(vo2_rows))

    # Training readiness and gear are intentionally out of scope for this pilot.

    for source_name, command in RANGE_SOURCES.items():
        try:
            value = fetch_range_chunked(cli, command, start, end, max_days=28)
            datasets[source_name] = records_from_indexed(value)
            LOG.info("%s: %d records", source_name, len(datasets[source_name]))
        except Exception as exc:
            errors.append({"source": source_name, "error": str(exc)})
            datasets[source_name] = []
            LOG.exception("%s failed", source_name)

    # Daily endpoints are queried by date, as required by Garmin's API.
    for source_name, command in (
        ("sleep_daily", "sleep"), ("heart_rate_daily", "heart-rate"),
        ("hrv_daily", "hrv"), ("spo2_daily", "spo2"),
        ("fitness_age_daily", "fitness-age"),
    ):
        rows = []
        cursor = start
        while cursor <= end:
            day = cursor.isoformat()
            try:
                value = cli.run(f"{command} {day}", command, "--date", day)
                if value is not None:
                    rows.append({"calendarDate": day, "value": value})
            except Exception as exc:
                LOG.warning("%s %s unavailable: %s", source_name, day, exc)
            cursor += timedelta(days=1)
        datasets[source_name] = rows
        LOG.info("%s: %d records", source_name, len(rows))

    con = connect()
    try:
        ensure_schema(con)
        total = 0
        for source_name, rows in datasets.items():
            total += load_records(con, source_name, rows, run_id)
            LOG.info("Loaded %s: %d rows", source_name, len(rows))
        counts = con.execute("""
            SELECT source_name, count(*) AS row_count
            FROM raw.garmin_payloads
            GROUP BY source_name ORDER BY source_name
        """).fetchall()
        LOG.info("Pilot run %s loaded %d rows; current RAW counts: %s",
                 run_id, total, counts)
        if errors:
            raise RuntimeError(f"Pilot completed with {len(errors)} source errors: {errors}")
        if not activities:
            raise RuntimeError("No activities returned for pilot window")
    finally:
        con.close()


if __name__ == "__main__":
    main()
