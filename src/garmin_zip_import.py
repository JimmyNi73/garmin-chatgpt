"""Import Garmin Connect ZIP wellness sources into MotherDuck for bounded validation."""
from __future__ import annotations

import json
import logging
import os
import uuid
import zipfile
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from motherduck_loader import connect, ensure_schema, load_records

LOG = logging.getLogger("garmin_zip_import")
TZ = ZoneInfo("Europe/Stockholm")


def read_json(zf: zipfile.ZipFile, name: str):
    return json.loads(zf.read(name))


def in_window(value, start: date, end: date) -> bool:
    try:
        return start <= date.fromisoformat(str(value)[:10]) <= end
    except (TypeError, ValueError):
        return False


def activity_date(row: dict) -> str | None:
    stamp = row.get("startTimeLocal") or row.get("startTimeGmt") or row.get("beginTimestamp")
    if stamp is None:
        return None
    try:
        if isinstance(stamp, (int, float)):
            return datetime.fromtimestamp(float(stamp) / 1000, TZ).date().isoformat()
        return str(stamp)[:10]
    except (ValueError, OSError, OverflowError):
        return None


def build_zip_records(zf: zipfile.ZipFile, start: date, end: date) -> dict[str, list[dict]]:
    records: dict[str, list[dict]] = {}

    def add(source: str, day: str | None, payload: dict, key: str | None = None):
        if day and in_window(day, start, end):
            row = {"calendarDate": day, "source_payload": payload}
            if key:
                row["zip_record_key"] = key
            records.setdefault(source, []).append(row)

    # Weight samples are separate records; do not collapse same-day weigh-ins.
    for name in zf.namelist():
        if name.endswith("3889253_userBioMetrics.json"):
            for i, row in enumerate(read_json(zf, name)):
                meta = row.get("metaData") or {}
                day = str(meta.get("calendarDate", ""))[:10]
                weight = row.get("weight")
                if isinstance(weight, dict) and weight.get("weight") is not None:
                    add("weight_measurements", day, row,
                        f"{day}:{meta.get('sequence', i)}")

        elif name.endswith("3889253_fitnessAgeData.json"):
            for i, row in enumerate(read_json(zf, name)):
                day = str(row.get("asOfDateGmt") or row.get("createTimestamp") or "")[:10]
                add("fitness_age", day, row, str(row.get("createTimestamp", i)))

        elif name.endswith("_sleepData.json"):
            for i, row in enumerate(read_json(zf, name)):
                day = row.get("calendarDate")
                add("zip_sleep", day, row, str(row.get("sleepStartTimestampGMT", i)))

        elif "TrainingReadinessDTO_" in name:
            for i, row in enumerate(read_json(zf, name)):
                add("training_readiness", row.get("calendarDate"), row,
                    str(row.get("timestamp", i)))

        elif "MetricsMaxMetData_" in name:
            for i, row in enumerate(read_json(zf, name)):
                add("vo2_max", row.get("calendarDate"), row,
                    f"{row.get('calendarDate')}:{row.get('sport')}:{row.get('updateTimestamp', i)}")

        elif name.endswith("_healthStatusData.json"):
            for i, row in enumerate(read_json(zf, name)):
                add("health_status", row.get("calendarDate"), row,
                    str(row.get("updateTimestampUTC", i)))

        elif "UDSFile_" in name:
            for i, row in enumerate(read_json(zf, name)):
                add("zip_daily_summaries", row.get("calendarDate"), row,
                    str(row.get("uuid", i)))

    return records


def activity_summaries(zf: zipfile.ZipFile, start: date, end: date) -> dict[str, dict]:
    found = {}
    for name in zf.namelist():
        if "summarizedActivities" not in name:
            continue
        payload = read_json(zf, name)
        if isinstance(payload, list):
            rows = [row for item in payload if isinstance(item, dict)
                    for row in item.get("summarizedActivitiesExport", [])]
        else:
            rows = payload.get("summarizedActivitiesExport", [])
        for row in rows:
            day = activity_date(row)
            if day and in_window(day, start, end):
                found[str(row.get("activityId"))] = row
    return found


def activity_ids(zf: zipfile.ZipFile, start: date, end: date) -> set[str]:
    return set(activity_summaries(zf, start, end))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    zip_path = Path(os.environ.get("GARMIN_VALIDATION_ZIP", "Garmin_Connect_Export_2026-11-06.zip"))
    if not zip_path.is_file():
        raise FileNotFoundError(f"Validation ZIP not found: {zip_path}")
    windows = [
        ("2011-12", date(2011, 12, 1), date(2011, 12, 31)),
        ("2022-05", date(2022, 5, 1), date(2022, 5, 31)),
        ("latest_zip", date(2026, 10, 29), date(2026, 11, 6)),
    ]
    run_id = "zip-" + str(uuid.uuid4())
    with zipfile.ZipFile(zip_path) as zf:
        con = connect()
        try:
            ensure_schema(con)
            expected = {}
            for label, start, end in windows:
                expected[label] = {
                    "activities": activity_summaries(zf, start, end),
                    "sources": build_zip_records(zf, start, end),
                }
                for source, rows in expected[label]["sources"].items():
                    # Stable keys preserve multiple measurements on the same date.
                    unique = {}
                    for i, row in enumerate(rows):
                        key = row.get("zip_record_key") or f"{row['calendarDate']}:{i}"
                        row["zip_record_key"] = key
                        unique[key] = row
                    load_records(con, source, unique.values(), run_id)

            # Validate the two requested historical months against ZIP activity IDs
            # and the ZIP wellness records now represented in CORE.
            failures = []
            for label, start, end in windows[:2]:
                exp_rows = expected[label]["activities"]\n                exp_ids = set(exp_rows)
                db_ids = {
                    r[0] for r in con.execute(
                        "SELECT activity_id FROM core.activities WHERE activity_date BETWEEN ? AND ?",
                        [start, end]).fetchall()
                }
                missing = sorted(exp_ids - db_ids)
                if missing:
                    failures.append(f"{label}: {len(missing)} ZIP activities missing from CORE: {missing[:10]}")
                for aid, source_row in exp_rows.items():
                    db_row = con.execute(
                        """SELECT activity_date, distance_m, duration_s, average_hr, max_hr, calories
                           FROM core.activities WHERE activity_id=?""", [aid]).fetchone()
                    if not db_row:
                        continue
                    expected_values = (
                        activity_date(source_row),
                        (float(source_row["distance"]) / 100.0) if source_row.get("distance") is not None else None,
                        (float(source_row["duration"]) / 1000.0) if source_row.get("duration") is not None else None,
                        float(source_row["avgHr"]) if source_row.get("avgHr") is not None else None,
                        float(source_row["maxHr"]) if source_row.get("maxHr") is not None else None,
                        (float(source_row["calories"]) / 4.184) if source_row.get("calories") is not None else None,
                    )
                    actual_values = (
                        str(db_row[0]) if db_row[0] is not None else None,
                        *[float(v) if v is not None else None for v in db_row[1:]],
                    )
                    for field, expected_value, actual_value in zip(
                        ("date", "distance_m", "duration_s", "average_hr", "max_hr", "calories"),
                        expected_values, actual_values,
                    ):
                        if expected_value is None or actual_value is None:
                            if expected_value != actual_value:
                                failures.append(f"{label}/{aid}/{field}: ZIP={expected_value}, DB={actual_value}")
                        elif abs(float(expected_value) - float(actual_value)) > 0.05:
                            failures.append(f"{label}/{aid}/{field}: ZIP={expected_value}, DB={actual_value}")
                for source, rows in expected[label]["sources"].items():
                    actual = con.execute(
                        "SELECT count(*) FROM core.daily_metrics WHERE metric_source=? AND metric_date BETWEEN ? AND ?",
                        [source, start, end]).fetchone()[0]
                    if actual < len(rows):
                        failures.append(f"{label}/{source}: expected at least {len(rows)} rows, found {actual}")
                LOG.info("%s ZIP activities=%d DB activities=%d source_counts=%s",
                         label, len(exp_ids), len(db_ids),
                         {k: len(v) for k, v in expected[label]["sources"].items()})
            if failures:
                raise RuntimeError("ZIP validation failed:\n- " + "\n- ".join(failures))
            LOG.info("ZIP validation passed for both historical test months.")
        finally:
            con.close()


if __name__ == "__main__":
    main()
