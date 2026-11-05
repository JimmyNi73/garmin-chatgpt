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
# Dropbox OAuth app uses an app-folder root: "/" is already the GarminChatGPT app folder.
# Dropbox UI displays this under Apps/GarminChatGPT, but SDK paths are relative to it.
HISTORY_DIR = "/History"
CURRENT_DIR = "/Current"
CURRENT_PATH = f"{CURRENT_DIR}/garmin_current.json"
HISTORY_PROGRESS_PATH = "/garmin_history_progress.json"
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
    return local.minute < 30 and local.hour in (6, 8, 14, 16, 20)


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


def dropbox_file_exists(dbx: dropbox.Dropbox, path: str) -> bool:
    try:
        dbx.files_get_metadata(path)
        return True
    except dropbox.exceptions.ApiError as exc:
        if exc.error.is_path() and exc.error.get_path().is_not_found():
            return False
        raise


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


class HistoryIncomplete(RuntimeError):
    """History export has retryable failed activities and must resume later."""


def activity_records_resumable(
    cli: GarminCLI, summaries: list[dict[str, Any]], dbx: dropbox.Dropbox
) -> list[dict[str, Any]]:
    progress = read_dropbox(dbx, HISTORY_PROGRESS_PATH) or {}
    completed = progress.get("activities", {})
    if not isinstance(completed, dict):
        completed = {}
    expected_ids = {activity_id(x) for x in summaries if activity_id(x)}
    # Ignore stale checkpoint records that are not part of the current Garmin index.
    completed = {aid: row for aid, row in completed.items() if aid in expected_ids}
    failed: dict[str, str] = {}
    newly_completed = 0

    LOG.info("History resume: %d/%d activities already completed",
             len(completed), len(summaries))

    for i, summary in enumerate(summaries, 1):
        aid = activity_id(summary)
        if not aid or aid in completed:
            continue

        last_error = ""
        for attempt in range(1, 4):
            try:
                detail = cli.run(f"activity {aid}", "activity", aid)
                workouts = cli.run(f"activity-workouts {aid}",
                                   "activity-workouts", aid)
                completed[aid] = {
                    "summary": summary, "detail": detail, "workouts": workouts
                }
                newly_completed += 1
                failed.pop(aid, None)
                break
            except Exception as exc:
                last_error = str(exc)
                LOG.warning("Activity %s failed (attempt %d/3): %s",
                            aid, attempt, last_error)
                if attempt < 3:
                    time.sleep(5 * attempt)
        else:
            failed[aid] = last_error
            LOG.error("Skipping activity %s after 3 attempts; will retry next run", aid)

        if newly_completed and newly_completed % 25 == 0:
            upload_json(dbx, HISTORY_PROGRESS_PATH, {
                "schema_version": 1,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "activities": completed,
            })
            LOG.info("History checkpoint saved: %d/%d activities",
                     len(completed), len(summaries))
        if i % 15 == 0:
            LOG.info("History details processed: %d/%d; completed %d; failed %d",
                     i, len(summaries), len(completed), len(failed))
            time.sleep(BATCH_PAUSE)

    # Always checkpoint at the end, including failures, so successful work survives.
    upload_json(dbx, HISTORY_PROGRESS_PATH, {
        "schema_version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "activities": completed,
    })

    if failed:
        raise HistoryIncomplete(
            f"{len(failed)} activities failed after 3 attempts; "
            f"{len(completed)}/{len(summaries)} saved for resume"
        )
    if set(completed) != expected_ids:
        raise HistoryIncomplete(
            f"Only {len(completed)}/{len(expected_ids)} indexed activities completed; resume required"
        )
    return list(completed.values())


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


def build_history(cli: GarminCLI, today: date, dbx: dropbox.Dropbox) -> dict[str, Any]:
    start = "2000-01-01"
    end = today.isoformat()
    LOG.info("Creating history from %s through %s", start, end)
    summaries = fetch_activities(cli, start, end)
    records = activity_records_resumable(cli, summaries, dbx)
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


def activity_records(cli: GarminCLI, summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records = []
    for summary in summaries:
        aid = activity_id(summary)
        if not aid: raise RuntimeError("Activity summary missing ID")
        error = None
        for attempt in range(1, 4):
            try:
                records.append({"summary": summary, "detail": cli.run(f"activity {aid}", "activity", aid),
                                "workouts": cli.run(f"activity-workouts {aid}", "activity-workouts", aid)})
                break
            except Exception as exc:
                error = exc
                LOG.warning("Activity %s failed attempt %d/3: %s", aid, attempt, exc)
                if attempt < 3: time.sleep(5 * attempt)
        else: raise RuntimeError(f"Activity {aid} failed after 3 attempts: {error}")
    return records
def build_current(cli: GarminCLI, today: date, old_current: dict[str, Any] | None,
                  activity_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    activity_start = (today - timedelta(days=CURRENT_ACTIVITY_DAYS - 1)
                      if old_current else date(today.year, 1, 1))
    range_start = today - timedelta(days=RANGE_DAYS - 1)
    daily_start = today - timedelta(days=DAILY_DETAIL_DAYS - 1)
    errors: list[dict[str, str]] = []
    if activity_rows is None:
        summaries = fetch_activities(cli, activity_start.isoformat(), today.isoformat())
        activities = activity_records(cli, summaries)
    else:
        activities = activity_rows
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


def ensure_dropbox_folders(dbx: dropbox.Dropbox) -> None:
    """Require the user's existing folders; never create Dropbox folders."""
    for path in ("/Backup", HISTORY_DIR, CURRENT_DIR):
        try:
            metadata = dbx.files_get_metadata(path)
        except dropbox.exceptions.ApiError as exc:
            raise RuntimeError(f"Required Dropbox folder is missing: {path}") from exc
        if not isinstance(metadata, dropbox.files.FolderMetadata):
            raise RuntimeError(f"Expected a Dropbox folder at {path}")


def year_of_activity(row: dict[str, Any]) -> int:
    summary = row.get("summary") if isinstance(row.get("summary"), dict) else {}
    detail = row.get("detail") if isinstance(row.get("detail"), dict) else {}
    raw = next((obj[key] for obj in (summary, detail, row)
                for key in ("startTimeLocal", "startTimeGMT", "start_time", "startTime")
                if obj.get(key)), None)
    if raw is None:
        raise RuntimeError(f"Activity {activity_id(summary)} has no usable date")
    try:
        if isinstance(raw, (int, float)):
            stamp = raw / (1000 if raw > 10**11 else 1)
            return datetime.fromtimestamp(stamp, timezone.utc).astimezone(TZ).year
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.year
        return parsed.astimezone(TZ).year
    except (ValueError, TypeError, OverflowError) as exc:
        raise RuntimeError(f"Unparseable activity date: {raw}") from exc


def annual_payload(year: int, rows: list[dict[str, Any]], profile: Any,
                   zones: Any) -> dict[str, Any]:
    ids = [activity_id(x.get("summary", {})) for x in rows]
    if None in ids or len(ids) != len(set(ids)):
        raise RuntimeError(f"Invalid or duplicate activity IDs in {year}")
    return {
        "schema_version": 1,
        "metadata": {
            "file_role": "immutable_activity_history",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "activity_count": len(rows),
            "export_status": "complete",
            "source": "Garmin Connect",
        },
        "period": {"start_date": f"{year}-01-01", "end_date": f"{year}-12-31"},
        "profile": profile,
        "heart_rate_zones": zones,
        "activities": rows,
    }


def save_annual_file(dbx: dropbox.Dropbox, year: int,
                     rows: list[dict[str, Any]], profile: Any, zones: Any) -> None:
    path = f"{HISTORY_DIR}/garmin_history_{year}.json"
    payload = annual_payload(year, rows, profile, zones)
    if dropbox_file_exists(dbx, path):
        existing = read_dropbox(dbx, path) or {}
        existing_rows = existing.get("activities", [])
        existing_ids = {activity_id(x.get("summary", {})) for x in existing_rows
                        if isinstance(x, dict)}
        expected_ids = {activity_id(x.get("summary", {})) for x in rows}
        if existing_ids != expected_ids:
            raise RuntimeError(
                f"Existing {year} archive differs from Garmin "
                f"(existing={len(existing_ids)}, expected={len(expected_ids)}); "
                "will not overwrite it automatically"
            )
        LOG.info("Verified immutable annual file %s (%d activities)", path, len(rows))
        return
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    dbx.files_upload(data, path, mode=WriteMode.add, mute=True)
    LOG.info("Created %s (%d activities, %d bytes)", path, len(rows), len(data))


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    if not should_run():
        LOG.info("Scheduled invocation outside allowed Stockholm time slots; skipping.")
        return
    if not os.getenv("GARMIN_PERSONAL_API_KEY"):
        raise RuntimeError("GARMIN_PERSONAL_API_KEY is missing")

    dbx = dbx_client()
    dbx.users_get_current_account()
    LOG.info("Dropbox authentication OK.")
    ensure_dropbox_folders(dbx)

    today = datetime.now(TZ).date()
    current_year = today.year
    cli = GarminCLI()

    # The Backup file is deliberately not read, migrated, or modified.
    LOG.info("Fetching complete Garmin activity index from 2000-01-01 through %s",
             today.isoformat())
    summaries = fetch_activities(cli, "2000-01-01", today.isoformat())
    if not summaries:
        raise RuntimeError("Garmin returned zero activities; refusing to publish empty exports")
    summary_ids = [activity_id(x) for x in summaries]
    if any(x is None for x in summary_ids) or len(summary_ids) != len(set(summary_ids)):
        raise RuntimeError("Garmin activity index contains missing or duplicate IDs")

    records = activity_records_resumable(cli, summaries, dbx)
    by_year: dict[int, list[dict[str, Any]]] = {}
    for row in records:
        year = year_of_activity(row)
        if year > current_year:
            raise RuntimeError(f"Garmin returned a future-dated activity in {year}")
        by_year.setdefault(year, []).append(row)

    # All details must be fetched before any year can be marked complete.
    profile_ok, profile = optional(cli, [], "profile-settings", "profile-settings")
    zones_ok, zones = optional(cli, [], "heart-rate-zones", "heart-rate-zones")
    if not profile_ok:
        LOG.warning("Profile settings unavailable; continuing with null profile.")
    if not zones_ok:
        LOG.warning("Heart-rate zones unavailable; continuing with null zones.")

    for year in sorted(y for y in by_year if y < current_year):
        save_annual_file(dbx, year, by_year[year], profile, zones)

    # Current contains this calendar year only. Keep prior current-year daily
    # metrics where possible, but never carry activities across a year boundary.
    old_current = read_dropbox(dbx, CURRENT_PATH)
    if old_current:
        old_period = old_current.get("period", {})
        old_start = str(old_period.get("start_date", ""))
        if not old_start.startswith(f"{current_year}-"):
            old_current = None

    current_year_rows = sorted(
        by_year.get(current_year, []),
        key=lambda x: str(x.get("summary", {}).get("startTimeLocal", "")))
    current = build_current(cli, today, old_current, current_year_rows)
    # Preserve previously collected daily dates that fall outside the rolling
    # detail window; the activity list itself is already the complete year-to-date set.
    current = merge_current(old_current, current)
    current["period"]["start_date"] = f"{current_year}-01-01"
    current["period"]["end_date"] = today.isoformat()
    current["metadata"]["activity_window_days"] = (today - date(current_year, 1, 1)).days + 1
    current["metadata"]["activity_count"] = len(current["activities"])
    current["metadata"]["source"] = "Garmin Connect"
    current["metadata"]["export_status"] = "complete" if not current.get("source_errors") else "partial"
    current["metadata"]["garmin_api_requests"] = cli.request_count

    encoded = json.dumps(current, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    max_bytes = 4 * 1024 * 1024
    if len(encoded) > max_bytes:
        raise RuntimeError(
            f"Current export is {len(encoded)} bytes, above the 4 MiB safety limit; "
            "refusing upload"
        )
    # Optional wellness endpoint failures are recorded, but do not discard
    # successfully retrieved activity data.
    upload_json(dbx, CURRENT_PATH, current)
    LOG.info("Finished. Years archived: %s; current year %d activities: %d; "
             "Garmin requests: %d",
             ", ".join(map(str, sorted(y for y in by_year if y < current_year))),
             current_year, len(current["activities"]), cli.request_count)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOG.exception("Garmin export failed.")
        raise
