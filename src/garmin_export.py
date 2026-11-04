
#!/usr/bin/env python3
"""Export Garmin Connect Personal API data to JSON."""

import json
import logging
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

LOG = logging.getLogger("garmin_export")

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "export"))
EXPORT_DAYS = int(os.getenv("EXPORT_DAYS", "730"))
DAILY_EXPORT_DAYS = int(os.getenv("DAILY_EXPORT_DAYS", "30"))
PAGE_SIZE = 100

# Commands accepting a date range.
RANGE_COMMANDS = [
    "stress",
    "steps",
    "hydration",
    "effective-weight-goals",
    "weight-measurements",
    "sleep-summaries",
    "body-battery",
    "daily-summaries",
    "hrv-summaries",
    "wellness-activity-summaries",
]

# Commands accepting one calendar date.
DAILY_COMMANDS = [
    "food-logs",
    "sleep",
    "heart-rate",
    "hrv",
    "fitness-age",
    "spo2",
]

# Commands requiring no date.
SINGLE_COMMANDS = [
    "profile-settings",
    "heart-rate-zones",
]


def run_garmin(args):
    """Run Garmin CLI and return decoded JSON."""
    command = ["gc-personal", "--quiet", *args]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )

    if result.returncode != 0:
        message = result.stderr.strip() or "Unknown Garmin error"
        raise RuntimeError(message)

    output = result.stdout.strip()

    if not output:
        return None

    return json.loads(output)


def collect(name, args, errors):
    """Run one export and record errors without stopping everything."""
    LOG.info("Exporting %s", name)

    try:
        return run_garmin(args)
    except Exception as exc:
        LOG.error("%s failed: %s", name, exc)
        errors.append({
            "dataset": name,
            "error": str(exc),
        })
        return None


def export_activities(start, end, errors):
    """Retrieve activities using pagination."""
    activities = []
    offset = 0

    while True:
        page = collect(
            f"activities page {offset}",
            [
                "activities",
                "--start", str(offset),
                "--limit", str(PAGE_SIZE),
                "--start-date", start,
                "--end-date", end,
            ],
            errors,
        )

        if not isinstance(page, list) or not page:
            break

        activities.extend(page)
        LOG.info("Retrieved %s activities", len(activities))

        if len(page) < PAGE_SIZE:
            break

        offset += len(page)

    return activities


def export_activity_details(activities, errors):
    """Retrieve details and workout sets for each activity."""
    details = {}
    workouts = {}

    for index, activity in enumerate(activities, start=1):
        activity_id = activity.get("activityId")

        if not activity_id:
            continue

        activity_id = str(activity_id)
        LOG.info(
            "Activity details %s/%s: %s",
            index,
            len(activities),
            activity_id,
        )

        detail = collect(
            f"activity {activity_id}",
            ["activity", activity_id],
            errors,
        )

        if detail is not None:
            details[activity_id] = detail

        workout = collect(
            f"activity-workouts {activity_id}",
            ["activity-workouts", activity_id],
            errors,
        )

        if workout is not None:
            workouts[activity_id] = workout

    return details, workouts


def export_daily_history(command, start_day, end_day, errors):
    """Export a command once per day."""
    result = {}

    current = start_day

    while current <= end_day:
        day = current.isoformat()

        data = collect(
            f"{command} {day}",
            [command, "--date", day],
            errors,
        )

        if data is not None:
            result[day] = data

        current += timedelta(days=1)

    return result


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not os.getenv("GARMIN_PERSONAL_API_KEY"):
        raise RuntimeError(
            "GARMIN_PERSONAL_API_KEY environment variable is missing"
        )

    if EXPORT_DAYS < 1 or DAILY_EXPORT_DAYS < 1:
        raise ValueError("Export periods must be at least one day")

    today = date.today()
    range_start = today - timedelta(days=EXPORT_DAYS - 1)
    daily_start = today - timedelta(days=DAILY_EXPORT_DAYS - 1)

    export = {
        "metadata": {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "range_start": range_start.isoformat(),
            "range_end": today.isoformat(),
            "daily_history_start": daily_start.isoformat(),
            "daily_history_end": today.isoformat(),
            "export_days": EXPORT_DAYS,
            "daily_export_days": DAILY_EXPORT_DAYS,
        },
        "data": {},
        "errors": [],
    }

    errors = export["errors"]
    start = range_start.isoformat()
    end = today.isoformat()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Activities.
    activities = export_activities(start, end, errors)
    export["data"]["activities"] = activities

    # Individual activity details and strength-training sets.
    details, workouts = export_activity_details(activities, errors)
    export["data"]["activity_details"] = details
    export["data"]["activity_workouts"] = workouts

    # Date-range datasets.
    for command in RANGE_COMMANDS:
        data = collect(
            command,
            [
                command,
                "--start-date", start,
                "--end-date", end,
            ],
            errors,
        )

        if data is not None:
            export["data"][command] = data

    # Daily datasets (30 days by default).
    for command in DAILY_COMMANDS:
        export["data"][command] = export_daily_history(
            command,
            daily_start,
            today,
            errors,
        )

    # Datasets without date parameters.
    for command in SINGLE_COMMANDS:
        data = collect(command, [command], errors)

        if data is not None:
            export["data"][command] = data

    output_file = OUTPUT_DIR / "garmin_latest.json"

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(
            export,
            file,
            ensure_ascii=False,
            indent=2,
        )

    LOG.info("Export completed: %s", output_file)
    LOG.info("Activities: %s", len(activities))
    LOG.info("Errors: %s", len(errors))
    LOG.info("File size: %s bytes", output_file.stat().st_size)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Garmin export failed")
        sys.exit(1)
