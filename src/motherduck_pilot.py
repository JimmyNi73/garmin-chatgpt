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

    with PersonalApiAuthenticator() as auth:
        token = auth.obtain_token(resolve_api_key())
        with GarminPersonalApiClient(token.access_token) as api:
            profile = api.get("/userprofile-service/userprofile/settings")
            def find_profile_id(node):
                if isinstance(node, dict):
                    for key in ("userProfilePk", "userProfileId"):
                        if node.get(key) is not None:
                            return node[key]
                    for value in node.values():
                        found = find_profile_id(value)
                        if found is not None:
                            return found
                elif isinstance(node, list):
                    for value in node:
                        found = find_profile_id(value)
                        if found is not None:
                            return found
                return None

            profile_id = find_profile_id(profile)
            # Garmin's settings endpoint omits the profile key for this account.
            # The profile PK was confirmed from the authenticated Connect response
            # during earlier pilot diagnostics; allow an environment override.
            profile_id = profile_id or os.getenv("GARMIN_USER_PROFILE_PK") or "3889253"
            LOG.info("Using Garmin profile key for gear lookup")
            gear_registry = {}
            gear_links = []
            try:
                gear_payload = api.get("/gear-service/gear/filterGear",
                                       params={"userProfilePk": str(profile_id)})
                # Garmin's live filterGear contract returns an ARRAY of gear rows.
                # Keep compatibility with the wrapped export-like shape too.
                if isinstance(gear_payload, list) and len(gear_payload) == 1 and isinstance(gear_payload[0], dict) and "gearDTOS" in gear_payload[0]:
                    gear_payload = gear_payload[0]
                if isinstance(gear_payload, dict):
                    gear_rows = gear_payload.get("gearDTOS", gear_payload.get("items", []))
                    links_by_gear = gear_payload.get("gearActivityDTOs", {})
                    if isinstance(links_by_gear, dict):
                        for gear_id, links in links_by_gear.items():
                            if isinstance(links, list):
                                for link in links:
                                    if isinstance(link, dict) and link.get("activityId") is not None:
                                        gear_links.append({"activityId": str(link["activityId"]),
                                                           "gear_id": str(link.get("gearPk", gear_id))})
                elif isinstance(gear_payload, list):
                    gear_rows = gear_payload
                else:
                    raise RuntimeError("Garmin gear endpoint returned an unexpected payload")
                if not isinstance(gear_rows, list):
                    raise RuntimeError("Garmin gear endpoint returned no gear list")
                for item in gear_rows:
                    if isinstance(item, dict):
                        gid = item.get("gearPk") or item.get("uuid") or item.get("gearUUID")
                        if gid is not None:
                            gear_registry[str(gid)] = item
            except Exception as gear_exc:
                LOG.warning("Garmin gear registry unavailable: %s", gear_exc)

            # The account-level endpoint returns gear only; activity assignments
            # are exposed by the same endpoint filtered by activityId.
            if gear_registry:
                for activity in activities:
                    summary = activity.get("summary", {})
                    aid = summary.get("activityId") or summary.get("activity_id")
                    if aid is None:
                        continue
                    try:
                        linked = api.get("/gear-service/gear/filterGear",
                                         params={"activityId": str(aid)})
                        linked_rows = linked if isinstance(linked, list) else ([linked] if isinstance(linked, dict) else [])
                        for gear in linked_rows:
                            if not isinstance(gear, dict):
                                continue
                            gid = gear.get("gearPk") or gear.get("uuid") or gear.get("gearUUID")
                            if gid is not None:
                                gear_registry.setdefault(str(gid), gear)
                                gear_links.append({"activityId": str(aid), "gear_id": str(gid)})
                    except Exception as exc:
                        LOG.warning("Gear lookup for activity %s unavailable: %s", aid, exc)
            datasets["gear_registry"] = list(gear_registry.values())
            datasets["gear_activity_links"] = gear_links
            LOG.info("Gear: %d registry entries, %d activity links",
                     len(datasets["gear_registry"]), len(gear_links))

            vo2_rows = []
            try:
                vo2_rows = normalize_vo2_response(
                    api.get(f"/metrics-service/metrics/maxmet/daily/{start_text}/{end_text}"))
            except Exception as exc:
                LOG.warning("VO2 range endpoint failed; trying per-day latest: %s", exc)
            if not vo2_rows:
                cursor = start
                while cursor <= end:
                    day = cursor.isoformat()
                    try:
                        vo2_rows.extend(normalize_vo2_response(
                            api.get(f"/metrics-service/metrics/maxmet/latest/{day}"), day))
                    except Exception as exc:
                        if "404" not in str(exc):
                            LOG.warning("VO2 %s failed: %s", day, exc)
                    cursor += timedelta(days=1)
            datasets["vo2_max_daily"] = vo2_rows
            LOG.info("VO2 max: %d measurements", len(vo2_rows))

            try:
                readiness = api.get(
                    f"/metrics-service/metrics/trainingreadiness/{start_text}/{end_text}")
                datasets["training_readiness_daily"] = (
                    readiness if isinstance(readiness, list) else
                    [readiness] if isinstance(readiness, dict) else [])
            except Exception as exc:
                LOG.warning("Training readiness range unavailable; trying daily endpoints: %s", exc)
                readiness_rows = []
                cursor = start
                while cursor <= end:
                    day = cursor.isoformat()
                    for path in (f"/metrics-service/metrics/trainingreadiness/{day}",
                                 f"/metrics-service/metrics/trainingreadiness/daily/{day}"):
                        try:
                            value = api.get(path)
                            if isinstance(value, list):
                                readiness_rows.extend(value)
                            elif isinstance(value, dict):
                                readiness_rows.append(value)
                            if any(isinstance(row, dict) and row.get("calendarDate") == day
                                   for row in readiness_rows):
                                break
                        except Exception as daily_exc:
                            if "404" not in str(daily_exc):
                                LOG.warning("Readiness %s failed: %s", day, daily_exc)
                    cursor += timedelta(days=1)
                datasets["training_readiness_daily"] = readiness_rows
            LOG.info("Training readiness: %d measurements",
                     len(datasets["training_readiness_daily"]))

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
