#!/usr/bin/env python3
"""Incremental Garmin Connect export to Dropbox."""
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
from zoneinfo import ZoneInfo

import dropbox
from dropbox.files import WriteMode

LOG = logging.getLogger("garmin_export")
TZ = ZoneInfo("Europe/Stockholm")
DROPBOX_DIR = "/Appar/GarminChatGPT"
HISTORY_PATH = f"{DROPBOX_DIR}/garmin_history.json"
CURRENT_PATH = f"{DROPBOX_DIR}/garmin_current.json"
OUTPUT_DIR = Path("export")
PAGE_SIZE = 100
REQUEST_PAUSE = float(os.getenv("GARMIN_REQUEST_PAUSE_SECONDS", "1"))
BATCH_PAUSE = float(os.getenv("GARMIN_BATCH_PAUSE_SECONDS", "20"))
MAX_RETRIES = int(os.getenv("GARMIN_MAX_RETRIES", "8"))
CURRENT_ACTIVITY_DAYS = int(os.getenv("GARMIN_CURRENT_ACTIVITY_DAYS", "30"))
RANGE_DAYS = int(os.getenv("GARMIN_RANGE_DAYS", "30"))
DAILY_DETAIL_DAYS = int(os.getenv("GARMIN_DAILY_DETAIL_DAYS", "14"))
REQUESTS_BETWEEN_PAUSES = 50


def should_run() -> bool:
    if os.getenv("GITHUB_EVENT_NAME") != "schedule":
        return True
    local = datetime.now(TZ)
    return local.minute < 30 and local.hour in (8, 14, 20)


def retry_after(stderr: str) -> int | None:
    m = re.search(r'"retryInSeconds"\s*:\s*(\d+)', stderr or "")
    if m:
        return max(1, int(m.group(1)))
    if re.search(r"HTTP\s*429|Too Many Requests", stderr or "", re.I):
        return 30
    return None


class GarminCLI:
    def __init__(self) -> None:
        self.request_count = 0

    def run(self, label: str, *args: str) -> Any:
        command = ["gc-personal", "--quiet", *args]
        for attempt in range(MAX_RETRIES + 1):
            if self.request_count:
                time.sleep(REQUEST_PAUSE)
            result = subprocess.run(command, capture_output=True, text=True,
                                    encoding="utf-8", errors="replace", timeout=180)
            if result.returncode == 0:
                self.request_count += 1
                if self.request_count % REQUESTS_BETWEEN_PAUSES == 0:
                    LOG.info("Completed %d Garmin requests; pausing %.0f sec",
                             self.request_count, BATCH_PAUSE)
                    time.sleep(BATCH_PAUSE)
                raw = result.stdout.strip()
                if not raw:
                    return None
                try:
                    return json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"{label}: invalid JSON: {exc}") from exc
            wait = retry_after(result.stderr)
            if wait is None or attempt >= MAX_RETRIES:
                raise RuntimeError(f"{label} failed: {result.stderr.strip()}")
            wait += random.uniform(0, 2)
            LOG.warning("%s rate limited; retrying in %.0f sec", label, wait)
            time.sleep(wait)
        raise RuntimeError(f"{label}: retry limit reached")


def dbx_client() -> dropbox.Dropbox:
    required = ("DROPBOX_APP_KEY", "DROPBOX_APP_SECRET", "DROPBOX_REFRESH_TOKEN")
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError("Missing Dropbox secrets: " + ", ".join(missing))
    return dropbox.Dropbox(
        oauth2_refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
        app_key=os.environ["DROPBOX_APP_KEY"],
        app_secret=os.environ["DROPBOX_APP_SECRET"],
        timeout=180,
    )


def read_dropbox(dbx: dropbox.Dropbox, path: str) -> dict[str, Any] | None:
    try:
        _, response = dbx.files_download(path)
    except dropbox.exceptions.ApiError as exc:
        if exc.error.is_path() and exc.error.get_path().is_not_found():
            return None
        raise
    try:
        value = json.loads(response.content)
        if not isinstance(value, dict):
            raise RuntimeError(f"{path} did not contain a JSON object")
        return value
    finally:
        response.close()


def upload_json(dbx: dropbox.Dropbox, path: str, value: dict[str, Any]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    local = OUTPUT_DIR / path.rsplit("/", 1)[-1]
    local.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n",
                     encoding="utf-8")
    dbx.files_upload(local.read_bytes(), path, mode=WriteMode.overwrite, mute=True)
    LOG.info("Uploaded %s (%d bytes)", path, local.stat().st_size)


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    raise RuntimeError(f"Expected array, got {type(value).__name__}")


def activity_id(item: dict[str, Any]) -> str | None:
    v = item.get("activityId", item.get("activity_id", item.get("id")))
    return str(v) if v is not None else None


def fetch_activities(cli: GarminCLI, start: str, end: str) -> list[dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    offset = 0
    while True:
        page = as_list(cli.run(
            f"activities page {offset}", "activities", "--start", str(offset),
            "--limit", str(PAGE_SIZE), "--start-date", start, "--end-date", end))
        for row in page:
            if isinstance(row, dict):
                aid = activity_id(row)
                if aid:
                    found[aid] = row
        LOG.info("Activity summaries collected: %d", len(found))
        if len(page) < PAGE_SIZE:
            break
        offset += len(page)
    return list(found.values())


def activity_records(cli: GarminCLI, summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records = []
    for i, summary in enumerate(summaries, 1):
        aid = activity_id(summary)
        if not aid:
            continue
        detail = cli.run(f"activity {aid}", "activity", aid)
        workouts = cli.run(f"activity-workouts {aid}", "activity-workouts", aid)
        records.append({"summary": summary, "detail": detail, "workouts": workouts})
        if i % 15 == 0:
            LOG.info("Activity details: %d/%d", i, len(summaries))
            time.sleep(BATCH_PAUSE)
    return records


def date_index(value: Any) -> dict[str, Any]:
    rows: list[tuple[str | None, Any]] = []
    if isinstance(value, list):
        rows = [(None, x) for x in value]
    elif isinstance(value, dict):
        fields = ("calendarDate", "date", "startDate", "summaryDate")
        if any(value.get(k) for k in fields):
            rows = [(None, value)]
        else:
            for k in ("dailySummaries", "daily_summaries", "summaries",
                      "data", "items", "values"):
                nested = value.get(k)
                if isinstance(nested, list):
                    rows = [(None, x) for x in nested]
                    break
                if isinstance(nested, dict):
                    rows = [(str(a), b) for a, b in nested.items()]
                    break
            if not rows:
                rows = [(str(k), v) for k, v in value.items() if isinstance(v, dict)]
    indexed = {}
    for supplied, row in rows:
        if not isinstance(row, dict):
            continue
        d = supplied[:10] if supplied else None
        if not d:
            for k in ("calendarDate", "date", "startDate", "summaryDate"):
                if row.get(k):
                    d = str(row[k])[:10]
                    break
        if d:
            indexed[d] = row
    return {"raw": value, "by_date": indexed}


def optional(cli: GarminCLI, errors: list[dict[str, str]],
             label: str, *args: str) -> tuple[bool, Any]:
    try:
        return True, cli.run(label, *args)
    except Exception as exc:
        LOG.error("%s failed: %s", label, exc)
        errors.append({"source": label, "error": str(exc)})
        return False, None


def merge_current(old: dict[str, Any] | None, new: dict[str, Any]) -> dict[str, Any]:
    if not old:
        return new
    old_activities = old.get("activities", [])
    by_id: dict[str, dict[str, Any]] = {}
    for item in old_activities if isinstance(old_activities, list) else []:
        summary = item.get("summary", {}) if isinstance(item, dict) else {}
        aid = activity_id(summary) if isinstance(summary, dict) else None
        if aid:
            by_id[aid] = item
    for item in new.get("activities", []):
        summary = item.get("summary", {})
        aid = activity_id(summary) if isinstance(summary, dict) else None
        if aid:
            by_id[aid] = item
    new["activities"] = sorted(
        by_id.values(),
        key=lambda x: str(x.get("summary", {}).get("startTimeLocal", "")))
    old_daily = old.get("daily", {})
    new_daily = new.get("daily", {})
    old_by_date = old_daily.get("by_date", {}) if isinstance(old_daily, dict) else {}
    old_range = old_daily.get("range_data", {}) if isinstance(old_daily, dict) else {}
    for d, values in old_by_date.items():
        new_daily.setdefault("by_date", {}).setdefault(d, values)
    for source, value in old_range.items():
        target = new_daily.setdefault("range_data", {}).setdefault(
            source, {"raw": None, "by_date": {}})
        for d, row in value.get("by_date", {}).items():
            target.setdefault("by_date", {}).setdefault(d, row)
    return new


def build_history(cli: GarminCLI, today: date) -> dict[str, Any]:
    start = "2000-01-01"
    end = today.isoformat()
    LOG.info("Creating history from %s through %s", start, end)
    summaries = fetch_activities(cli, start, end)
    records = activity_records(cli, summaries)
    errors: list[dict[str, str]] = []
    profile_ok, profile = optional(cli, errors, "profile-settings", "profile-settings")
    zones_ok, zones = optional(cli, errors, "heart-rate-zones", "heart-rate-zones")
    return {
        "schema_version": 1,
        "metadata": {
            "file_role": "immutable_activity_history",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "activity_count": len(records),
            "garmin_api_requests": cli.request_count,
            "source_errors": errors,
            "export_status": "complete" if not errors else "partial",
            "note": "Created only when this file is absent from Dropbox. Delete it manually to rebuild.",
        },
        "period": {"start_date": start, "end_date": end},
        "profile": profile if profile_ok else None,
        "heart_rate_zones": zones if zones_ok else None,
        "activities": records,
    }


def build_current(cli: GarminCLI, today: date) -> dict[str, Any]:
    activity_start = today - timedelta(days=CURRENT_ACTIVITY_DAYS - 1)
    range_start = today - timedelta(days=RANGE_DAYS - 1)
    daily_start = today - timedelta(days=DAILY_DETAIL_DAYS - 1)
    errors: list[dict[str, str]] = []
    summaries = fetch_activities(cli, activity_start.isoformat(), today.isoformat())
    activities = activity_records(cli, summaries)
    current: dict[str, Any] = {
        "schema_version": 1,
        "metadata": {
            "file_role": "rolling_current_data",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "activity_window_days": CURRENT_ACTIVITY_DAYS,
            "range_window_days": RANGE_DAYS,
            "daily_detail_window_days": DAILY_DETAIL_DAYS,
        },
        "period": {"start_date": activity_start.isoformat(), "end_date": today.isoformat()},
        "profile": None,
        "heart_rate_zones": None,
        "source_status": {},
        "source_errors": errors,
        "activities": activities,
        "daily": {"range_data": {}, "by_date": {},
                  "single_day_window": {"start_date": daily_start.isoformat(),
                                        "end_date": today.isoformat()}},
    }
    for field, command in (("profile", "profile-settings"),
                           ("heart_rate_zones", "heart-rate-zones")):
        ok, value = optional(cli, errors, command, command)
        current[field] = value
        current["source_status"][command] = "ok" if ok else "error"
    for field, command in {
        "stress": "stress", "sleep_summaries": "sleep-summaries",
        "body_battery": "body-battery", "daily_summaries": "daily-summaries",
        "hrv_summaries": "hrv-summaries",
        "wellness_activity_summaries": "wellness-activity-summaries",
    }.items():
        ok, value = optional(cli, errors, command, command, "--start-date",
                             range_start.isoformat(), "--end-date", today.isoformat())
        current["daily"]["range_data"][field] = (
            date_index(value) if ok else {"raw": None, "by_date": {}})
        current["source_status"][command] = "ok" if ok else "error"
    for offset in range(DAILY_DETAIL_DAYS):
        d = (daily_start + timedelta(days=offset)).isoformat()
        current["daily"]["by_date"][d] = {}
        for field, command in (("heart_rate", "heart-rate"), ("hrv", "hrv"),
                               ("spo2", "spo2")):
            ok, value = optional(cli, errors, f"{command} {d}", command, "--date", d)
            current["daily"]["by_date"][d][field] = (
                value if ok else {"error": "source request failed"})
            current["source_status"][f"{command} {d}"] = "ok" if ok else "error"
    current["metadata"]["garmin_api_requests"] = cli.request_count
    current["metadata"]["activity_count"] = len(activities)
    current["metadata"]["source_error_count"] = len(errors)
    current["metadata"]["export_status"] = "complete" if not errors else "partial"
    return current


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    if not should_run():
        LOG.info("Scheduled invocation outside 08:00, 14:00, 20:00 Stockholm time; skipping.")
        return
    if not os.getenv("GARMIN_PERSONAL_API_KEY"):
        raise RuntimeError("GARMIN_PERSONAL_API_KEY is missing")
    dbx = dbx_client()
    account = dbx.users_get_current_account()
    LOG.info("Dropbox authentication OK for configured account.")
    today = datetime.now(TZ).date()
    cli = GarminCLI()

    # History is immutable: create it only if Dropbox confirms it is absent.
    history = read_dropbox(dbx, HISTORY_PATH)
    if history is None:
        LOG.info("History file is absent; generating it once.")
        history = build_history(cli, today)
        upload_json(dbx, HISTORY_PATH, history)
    else:
        LOG.info("History file already exists; leaving it untouched.")

    old_current = read_dropbox(dbx, CURRENT_PATH)
    current = build_current(cli, today)
    current = merge_current(old_current, current)
    current["metadata"]["updated_at"] = datetime.now(timezone.utc).isoformat()
    upload_json(dbx, CURRENT_PATH, current)
    LOG.info("Finished. Account verified; current activities: %d; Garmin requests: %d",
             len(current.get("activities", [])), cli.request_count)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOG.exception("Garmin export failed.")
        raise
