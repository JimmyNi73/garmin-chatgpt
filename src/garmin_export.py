
#!/usr/bin/env python3
"""
Garmin Connect Personal API export.

Environment variables:
    GARMIN_PERSONAL_API_KEY  Garmin Personal API key
    EXPORT_DAYS             History window for daily metrics (default: 730)
    OUTPUT_DIR              Output directory (default: export)
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
from datetime import date, timedelta, datetime, timezone
from pathlib import Path

LOG = logging.getLogger("garmin_export")

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "export"))
EXPORT_DAYS = int(os.getenv("EXPORT_DAYS", "730"))
ACTIVITY_PAGE_SIZE = 100

# Commands that return date-based information.
DATE_RANGE_COMMANDS = [
    "stress",
    "steps",
    "hydration",
    "weight-measurements",
    "daily-summaries",
    "heart-rate",
    "hrv-summaries",
    "heart-rate-zones",
    "wellness-activity-summaries",
]

# Commands that return one day's information.
DAILY_COMMANDS = [
    "sleep",
    "sleep-summaries",
    "body-battery",
    "hrv",
    "spo2",
    "fitness-age",
]

# Commands without date parameters.
SINGLE_COMMANDS = [
    "profile-settings",
]


def run_cli(args: list[str], allow_failure: bool = False):
    """Run gc-personal and parse its JSON output."""
    command = ["gc-personal", "--quiet", *args]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if result.returncode != 0:
        message = result.stderr.strip() or "Unknown Garmin CLI error"
        if allow_failure:
            LOG.warning("%s: %s", " ".join(args), message)
            return None
        raise RuntimeError(message)

    output = result.stdout.strip()
    if not output:
        return None

    try:
        return json.loads(output)
    except json.JSONDecodeError:
        LOG.warning("Could not parse JSON from: %s", " ".join(args))
        return {"raw_output": output}


def get_supported_commands() -> set[str]:
    """Discover commands supported by the installed Garmin CLI."""
    result = subprocess.run(
        ["gc-personal", "--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if result.returncode != 0:
        raise RuntimeError("Could not read gc-personal --help")

    commands = set()

    for line in result.stdout.splitlines():
        match = re.match(r"^\s{2,}([a-z][a-z0-9-]+)\s{2,}", line)
        if match:
            commands.add(match.group(1))

    return commands


def get_command_help(command: str) -> str:
    result = subprocess.run(
        ["gc-personal", command, "--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return result.stdout + "\n" + result.stderr


def export_date_range(command: str, start: str, end: str):
    """Export a command that supports a date range."""
    return run_cli([
        command,
        "--start-date", start,
        "--end-date", end,
    ], allow_failure=True)


def export_daily(command: str, day: str):
    """Export a command that supports a single date."""
    return run_cli([
        command,
        "--date", day,
    ], allow_failure=True)


def export_activities(start: str, end: str):
    """Retrieve all activities using pagination."""
    activities = []
    offset = 0

    while True:
        page = run_cli([
            "activities",
            "--start", str(offset),
            "--limit", str(ACTIVITY_PAGE_SIZE),
            "--start-date", start,
            "--end-date", end,
        ])

        if not isinstance(page, list) or not page:
            break

        activities.extend(page)
        LOG.info("Activities retrieved: %s", len(activities))

        if len(page) < ACTIVITY_PAGE_SIZE:
            break

        offset += len(page)

    return activities


def export_activity_details(activities: list):
    """Retrieve individual activity details and workouts."""
    details = {}
    workouts = {}

    for index, activity in enumerate(activities, start=1):
        activity_id = activity.get("activityId")

        if not activity_id:
            continue

        key = str(activity_id)

        LOG.info(
            "Activity details %s/%s: %s",
            index,
            len(activities),
            key,
        )

        detail = run_cli(
            ["activity", key],
            allow_failure=True,
        )

        if detail is not None:
            details[key] = detail

        workout = run_cli(
            ["activity-workouts", key],
            allow_failure=True,
        )

        if workout is not None:
            workouts[key] = workout

    return details, workouts


def export_daily_history(command: str, start_day: date, end_day: date):
    """Export one-day commands over the selected history window."""
    result = {}

    current = start_day

    while current <= end_day:
        day = current.isoformat()

        data = export_daily(command, day)

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
        raise RuntimeError("GARMIN_PERSONAL_API_KEY is not configured")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    end_day = date.today()
    start_day = end_day - timedelta(days=EXPORT_DAYS)

    start = start_day.isoformat()
    end = end_day.isoformat()

    supported = get_supported_commands()

    export = {
        "metadata": {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "period_start": start,
            "period_end": end,
            "export_days": EXPORT_DAYS,
            "supported_commands": sorted(supported),
        },
        "data": {},
        "errors": [],
    }

    # Activities and individual activity details.
    if "activities" in supported:
        LOG.info("Exporting activities...")
        activities = export_activities(start, end)
        export["data"]["activities"] = activities

        if "activity" in supported or "activity-workouts" in supported:
            details, workouts = export_activity_details(activities)
            export["data"]["activity_details"] = details
            export["data"]["activity_workouts"] = workouts

    # Date-range commands.
    for command in DATE_RANGE_COMMANDS:
        if command not in supported:
            continue

        LOG.info("Exporting %s...", command)
        data = export_date_range(command, start, end)

        if data is not None:
            export["data"][command] = data
        else:
            export["errors"].append(command)

    # Daily commands.
    for command in DAILY_COMMANDS:
        if command not in supported:
            continue

        LOG.info("Exporting %s...", command)

        # Daily endpoints can be slow over long periods.
        # We retain the full requested history window.
        data = export_daily_history(command, start_day, end_day)

        export["data"][command] = data

    # Profile and other commands without date parameters.
    for command in SINGLE_COMMANDS:
        if command not in supported:
            continue

        LOG.info("Exporting %s...", command)
        data = run_cli([command], allow_failure=True)

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
    LOG.info("File size: %s bytes", output_file.stat().st_size)
    LOG.info("Activities: %s", len(export["data"].get("activities", [])))
    LOG.info("Errors: %s", len(export["errors"]))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOG.exception("Garmin export failed")
        sys.exit(1)

