"""14-day Garmin -> MotherDuck pilot. Reads live Garmin APIs only; no Dropbox or ZIP input."""
from __future__ import annotations

import logging
import os
import uuid
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
    indexed = date_index(value)
    return [
        {"calendarDate": day, **(row if isinstance(row, dict) else {"value": row})}
        for day, row in indexed["by_date"].items()
    ]


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
    try:
        gear_registry, activity_to_gear = fetch_gear_data(cli)
        enrich_gear_records(activities, gear_registry, activity_to_gear)
        LOG.info("Gear resolved: %d items, %d activity links",
                 len(gear_registry), len(activity_to_gear))
    except Exception:
        LOG.exception("Gear endpoint unavailable; pilot continues with gear omitted")

    datasets = {"activities": activities}
    errors = []
    # These range endpoints are not exposed by the existing gc-personal CLI
    # wrapper; call them with the same Garmin Personal API token flow as gear.
    with PersonalApiAuthenticator() as auth:
        token = auth.obtain_token(resolve_api_key())
        with GarminPersonalApiClient(token.access_token) as api:
            for source_name, endpoint in (
                ("vo2_max_daily", f"/metrics-service/metrics/maxmet/daily/{start_text}/{end_text}"),
                ("training_readiness_daily", f"/metrics-service/metrics/trainingreadiness/{start_text}/{end_text}"),
            ):
                try:
                    response = api.get(endpoint)
                    if isinstance(response, list):
                        datasets[source_name] = [row for row in response if isinstance(row, dict)]
                    elif isinstance(response, dict):
                        datasets[source_name] = [response]
                    else:
                        datasets[source_name] = []
                    LOG.info("%s: %d records", source_name, len(datasets[source_name]))
                except Exception as exc:
                    # Garmin currently returns 404 for these optional metrics
                    # endpoints on this account. Keep the pilot useful: record
                    # the source as empty, but do not fail a successful RAW load.
                    if "404" in str(exc):
                        datasets[source_name] = []
                        LOG.warning("%s endpoint returned 404; loading continues with no rows", source_name)
                    else:
                        errors.append({"source": source_name, "error": str(exc)})
                        LOG.exception("%s endpoint unavailable", source_name)

    for source_name, command in RANGE_SOURCES.items():
        ok, value = optional_range(
            cli, errors, [], source_name,
            command, "--start-date", start_text, "--end-date", end_text
        )
        datasets[source_name] = records_from_indexed(value) if ok and value is not None else []
        LOG.info("%s: %d records%s", source_name, len(datasets[source_name]),
                 "" if ok else " (API error)")

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
