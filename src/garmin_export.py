#!/usr/bin/env python3
"""Controlled Garmin export test. Produces export/garmin_test.json.

This is deliberately a bounded test run, not the production Dropbox sync yet.
It uses only documented gc-personal CLI commands and fails on incomplete datasets.
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import dropbox
from dropbox.files import WriteMode

LOG = logging.getLogger("garmin_export")
OUTPUT_DIR = Path("export")
OUTPUT_FILE = OUTPUT_DIR / "garmin_test.json"
PAGE_SIZE = 100
TEST_DAYS = int(os.getenv("GARMIN_TEST_DAYS", "30"))
DAILY_TEST_DAYS = int(os.getenv("GARMIN_DAILY_TEST_DAYS", "14"))
REQUEST_PAUSE = float(os.getenv("GARMIN_REQUEST_PAUSE_SECONDS", "1.0"))
BATCH_PAUSE = float(os.getenv("GARMIN_BATCH_PAUSE_SECONDS", "20"))
MAX_RETRIES = int(os.getenv("GARMIN_MAX_RETRIES", "8"))
REQUESTS_BETWEEN_PAUSES = 50


def retry_after(stderr: str) -> int | None:
    match = re.search(r'"retryInSeconds"\s*:\s*(\d+)', stderr or "")
    if match:
        return max(1, int(match.group(1)))
    match = re.search(r"HTTP\s*429|Too Many Requests", stderr or "", re.I)
    return 30 if match else None


class GarminCLI:
    def __init__(self) -> None:
        self.request_count = 0

    def run(self, label: str, *args: str) -> Any:
        command = ["gc-personal", "--quiet", *args]
        for attempt in range(MAX_RETRIES + 1):
            if self.request_count:
                time.sleep(REQUEST_PAUSE)
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=180,
            )
            if result.returncode == 0:
                self.request_count += 1
                if self.request_count % REQUESTS_BETWEEN_PAUSES == 0:
                    LOG.info("Completed %d Garmin requests; pausing %.0f seconds",
                             self.request_count, BATCH_PAUSE)
                    time.sleep(BATCH_PAUSE)
                raw = result.stdout.strip()
                if not raw:
                    return None
                try:
                    return json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"{label}: Garmin CLI returned invalid JSON: {exc}"
                    ) from exc

            wait = retry_after(result.stderr)
            if wait is None or attempt >= MAX_RETRIES:
                raise RuntimeError(
                    f"{label} failed (exit {result.returncode}): "
                    f"{result.stderr.strip() or result.stdout.strip()}"
                )
            wait += random.uniform(0, 2)
            LOG.warning("%s: rate limited; retrying in %.0f seconds", label, wait)
            time.sleep(wait)

        raise RuntimeError(f"{label}: retry limit reached")


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    raise RuntimeError(f"Expected a JSON array, got {type(value).__name__}")


def date_index(value: Any) -> dict[str, Any]:
    """Keep the original response and index recognizable daily rows for analysis."""
    rows = as_list(value)
    indexed: dict[str, Any] = {}
    for row in rows:
        if isinstance(row, dict):
            for key in ("calendarDate", "date", "startDate", "summaryDate"):
                if row.get(key):
                    indexed[str(row[key])[:10]] = row
                    break
    return {"raw": value, "by_date": indexed}


def activity_key(item: dict[str, Any]) -> str | None:
    value = item.get("activityId", item.get("activity_id", item.get("id")))
    return str(value) if value is not None else None


def optional_source(
    cli: GarminCLI,
    source_errors: list[dict[str, str]],
    label: str,
    *args: str,
) -> tuple[bool, Any]:
    """Fetch a non-critical source without aborting the complete export."""
    try:
        return True, cli.run(label, *args)
    except Exception as exc:
        message = str(exc)
        LOG.error("Optional source %s failed; continuing: %s", label, message)
        source_errors.append({"source": label, "error": message})
        return False, None


def upload_to_dropbox(local_path: Path, destination: str) -> None:
    """Upload a file to this app's Dropbox folder."""
    required = ("DROPBOX_APP_KEY", "DROPBOX_APP_SECRET", "DROPBOX_REFRESH_TOKEN")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError("Missing Dropbox GitHub secret(s): " + ", ".join(missing))

    # The Dropbox console's generated token is short-lived. It is used here
    # only for this one-off connectivity test, not for scheduled automation.
    dbx = dropbox.Dropbox(
        oauth2_access_token=os.environ["DROPBOX_REFRESH_TOKEN"],
        timeout=120,
    )
    account = dbx.users_get_current_account()
    metadata = dbx.files_upload(
        local_path.read_bytes(),
        destination,
        mode=WriteMode.overwrite,
        mute=True,
    )
    LOG.info(
        "Dropbox upload OK: %s (account %s; file id %s)",
        metadata.path_display or destination,
        account.email,
        metadata.id,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    if not os.getenv("GARMIN_PERSONAL_API_KEY"):
        raise RuntimeError("GARMIN_PERSONAL_API_KEY is missing")
    if TEST_DAYS < 1 or DAILY_TEST_DAYS < 1 or DAILY_TEST_DAYS > TEST_DAYS:
        raise ValueError("Require 1 <= GARMIN_DAILY_TEST_DAYS <= GARMIN_TEST_DAYS")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # Prove Dropbox authentication and write access before spending time on Garmin.
    smoke_file = OUTPUT_DIR / "dropbox_connection_test.json"
    smoke_file.write_text(
        json.dumps({
            "test": "GarminChatGPT Dropbox connection",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "Dropbox write test",
        }, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    upload_to_dropbox(smoke_file, "/dropbox_connection_test.json")

    today = date.today()
    start = today - timedelta(days=TEST_DAYS - 1)
    daily_start = today - timedelta(days=DAILY_TEST_DAYS - 1)
    start_s, end_s = start.isoformat(), today.isoformat()
    cli = GarminCLI()

    source_errors: list[dict[str, str]] = []
    source_status: dict[str, str] = {}
    export: dict[str, Any] = {
        "schema_version": 1,
        "metadata": {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "export_type": "bounded_test",
            "note": "Test window only; not the full history/current production export.",
        },
        "period": {"start_date": start_s, "end_date": end_s},
        "profile": None,
        "heart_rate_zones": None,
        "source_status": source_status,
        "source_errors": source_errors,
        "activities": [],
        "daily": {
            "range_data": {},
            "by_date": {},
            "single_day_window": {
                "start_date": daily_start.isoformat(),
                "end_date": end_s,
            },
        },
    }

    LOG.info("Exporting activity summaries %s to %s", start_s, end_s)
    offset = 0
    summaries: list[dict[str, Any]] = []
    while True:
        page = as_list(cli.run(
            f"activities page {offset}",
            "activities", "--start", str(offset), "--limit", str(PAGE_SIZE),
            "--start-date", start_s, "--end-date", end_s,
        ))
        summaries.extend(x for x in page if isinstance(x, dict))
        LOG.info("Retrieved %d activity summaries", len(summaries))
        if len(page) < PAGE_SIZE:
            break
        offset += len(page)

    for index, summary in enumerate(summaries, start=1):
        aid = activity_key(summary)
        if not aid:
            LOG.warning("Skipping activity summary without activity ID")
            continue
        detail = cli.run(f"activity {aid}", "activity", aid)
        workouts = cli.run(f"activity-workouts {aid}", "activity-workouts", aid)
        export["activities"].append({
            "summary": summary,
            "detail": detail,
            "workouts": workouts,
        })
        if index % 15 == 0:
            LOG.info("Fetched details for %d/%d activities", index, len(summaries))
            time.sleep(BATCH_PAUSE)

    # Profile and zone metadata are useful but must not block the export.
    for field, command in (
        ("profile", "profile-settings"),
        ("heart_rate_zones", "heart-rate-zones"),
    ):
        ok, value = optional_source(cli, source_errors, command, command)
        export[field] = value
        source_status[command] = "ok" if ok else "error"

    range_commands = {
        "stress": "stress",
        "sleep_summaries": "sleep-summaries",
        "body_battery": "body-battery",
        "daily_summaries": "daily-summaries",
        "hrv_summaries": "hrv-summaries",
        "wellness_activity_summaries": "wellness-activity-summaries",
    }
    for field, command in range_commands.items():
        LOG.info("Exporting %s", command)
        ok, value = optional_source(
            cli, source_errors, command, command,
            "--start-date", start_s, "--end-date", end_s,
        )
        export["daily"]["range_data"][field] = (
            date_index(value) if ok else
            {"raw": None, "by_date": {}, "error": "source request failed"}
        )
        source_status[command] = "ok" if ok else "error"

    daily_commands = {
        "heart_rate": "heart-rate",
        "hrv": "hrv",
        "spo2": "spo2",
    }
    for day_offset in range(DAILY_TEST_DAYS):
        day = daily_start + timedelta(days=day_offset)
        key = day.isoformat()
        export["daily"]["by_date"][key] = {}
        for field, command in daily_commands.items():
            label = f"{command} {key}"
            ok, value = optional_source(
                cli, source_errors, label, command, "--date", key
            )
            export["daily"]["by_date"][key][field] = (
                value if ok else {"error": "source request failed"}
            )
            source_status[label] = "ok" if ok else "error"

    export["metadata"]["garmin_api_requests"] = cli.request_count
    export["metadata"]["activity_count"] = len(export["activities"])
    export["metadata"]["daily_single_day_count"] = DAILY_TEST_DAYS
    export["metadata"]["source_error_count"] = len(source_errors)
    export["metadata"]["export_status"] = (
        "complete" if not source_errors else "partial"
    )
    with OUTPUT_FILE.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(export, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    LOG.info("Created %s (%d bytes)", OUTPUT_FILE, OUTPUT_FILE.stat().st_size)
    # Upload even when optional Garmin sources failed; the JSON records each failure.
    upload_to_dropbox(OUTPUT_FILE, "/garmin_test.json")
    LOG.info(
        "Activities: %d; Garmin requests: %d; optional source errors: %d",
        len(export["activities"]), cli.request_count, len(source_errors),
    )

if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOG.exception("Garmin test export failed; no incomplete test file should be analysed.")
        raise
