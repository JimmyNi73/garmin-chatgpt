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
    for path in (HISTORY_DIR, CURRENT_DIR):
        try:
            metadata = dbx.files_get_metadata(path)
        except dropbox.exceptions.ApiError as exc:
            raise RuntimeError(f"Required Dropbox folder is missing: {path}") from exc
        if not isinstance(metadata, dropbox.files.FolderMetadata):
            raise RuntimeError(f"Expected a Dropbox folder at {path}")


def activity_datetime(row: dict[str, Any]) -> datetime:
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
            return datetime.fromtimestamp(stamp, timezone.utc).astimezone(TZ)
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=TZ) if parsed.tzinfo is None else parsed.astimezone(TZ)
    except (ValueError, TypeError, OverflowError) as exc:
        raise RuntimeError(f"Unparseable activity date: {raw}") from exc


def year_of_activity(row: dict[str, Any]) -> int:
    return activity_datetime(row).year


def month_of_activity(row: dict[str, Any]) -> str:
    return activity_datetime(row).strftime("%Y-%m")


def month_bounds(month: str) -> tuple[str, str]:
    first = date.fromisoformat(month + "-01")
    following = date(first.year + (first.month == 12), first.month % 12 + 1, 1)
    return first.isoformat(), (following - timedelta(days=1)).isoformat()


def group_by_month(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(month_of_activity(row), []).append(row)
    return dict(sorted(grouped.items()))


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


def monthly_payload(month: str, rows: list[dict[str, Any]], profile: Any,
                    zones: Any, file_role: str) -> dict[str, Any]:
    ids = [activity_id(x.get("summary", {})) for x in rows]
    if None in ids or len(ids) != len(set(ids)):
        raise RuntimeError(f"Invalid or duplicate activity IDs in {month}")
    start, end = month_bounds(month)
    return {
        "schema_version": 1,
        "metadata": {"file_role": file_role, "created_at": datetime.now(timezone.utc).isoformat(),
                     "activity_count": len(rows), "export_status": "complete",
                     "source": "Garmin Connect", "month": month},
        "period": {"start_date": start, "end_date": end},
        "profile": profile, "heart_rate_zones": zones, "activities": rows,
    }


def verify_monthly_history_file(dbx: dropbox.Dropbox, month: str,
                                summaries: list[dict[str, Any]]) -> bool:
    path = f"{HISTORY_DIR}/garmin_history_{month}.json"
    if not dropbox_file_exists(dbx, path):
        return False
    existing = read_dropbox(dbx, path)
    if not existing or existing.get("metadata", {}).get("export_status") != "complete":
        raise RuntimeError(f"Existing {path} is not marked complete; refusing overwrite")
    rows = existing.get("activities")
    if not isinstance(rows, list):
        raise RuntimeError(f"Existing {path} has no valid activities array")
    old_ids = [activity_id(x.get("summary", {})) for x in rows if isinstance(x, dict)]
    expected = [activity_id(x) for x in summaries]
    if (len(old_ids) != len(rows) or None in old_ids or len(old_ids) != len(set(old_ids))
            or set(old_ids) != set(expected)):
        raise RuntimeError(f"Existing {path} differs from Garmin; refusing overwrite")
    LOG.info("MONTH %s: verified existing history; skipping details (%d activities)",
             month, len(expected))
    return True


def save_monthly_history_file(dbx: dropbox.Dropbox, month: str,
                              rows: list[dict[str, Any]], profile: Any, zones: Any) -> None:
    path = f"{HISTORY_DIR}/garmin_history_{month}.json"
    payload = monthly_payload(month, rows, profile, zones, "immutable_activity_history_monthly")
    if dropbox_file_exists(dbx, path):
        existing = read_dropbox(dbx, path) or {}
        old_ids = {activity_id(x.get("summary", {})) for x in existing.get("activities", [])
                   if isinstance(x, dict)}
        if old_ids != {activity_id(x.get("summary", {})) for x in rows}:
            raise RuntimeError(f"Existing {path} differs from Garmin; refusing overwrite")
        return
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    dbx.files_upload(data, path, mode=WriteMode.add, mute=True)
    LOG.info("Created %s (%d activities, %d bytes)", path, len(rows), len(data))


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


def verify_existing_annual_file(dbx: dropbox.Dropbox, year: int,
                                summaries: list[dict[str, Any]]) -> bool:
    """Return True only when an existing annual archive matches Garmin's index."""
    path = f"{HISTORY_DIR}/garmin_history_{year}.json"
    if not dropbox_file_exists(dbx, path):
        return False
    existing = read_dropbox(dbx, path)
    if not existing or existing.get("metadata", {}).get("export_status") != "complete":
        raise RuntimeError(f"Existing {path} is not marked complete; refusing to overwrite")
    rows = existing.get("activities")
    if not isinstance(rows, list):
        raise RuntimeError(f"Existing {path} has no valid activities array")
    existing_ids = [activity_id(x.get("summary", {})) for x in rows
                    if isinstance(x, dict) and isinstance(x.get("summary"), dict)]
    expected_ids = [activity_id(x) for x in summaries]
    if (None in existing_ids or len(existing_ids) != len(rows)
            or len(existing_ids) != len(set(existing_ids))
            or set(existing_ids) != set(expected_ids)):
        raise RuntimeError(
            f"Existing {path} does not match Garmin's activity index "
            f"(archive={len(existing_ids)}, Garmin={len(expected_ids)}); "
            "refusing automatic overwrite"
        )
    LOG.info("YEAR %d: existing archive verified; skipping detail downloads (%d activities)",
             year, len(expected_ids))
    return True


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
    archived_years: list[int] = []
    skipped_years: list[int] = []

    # Each historical year is a separate transaction: fetch its index, fetch all
    # details, validate, then publish before touching the next year.
    for year in range(2000, current_year):
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        LOG.info("YEAR %d: starting activity index fetch (%s to %s)",
                 year, start_date.isoformat(), end_date.isoformat())
        summaries = fetch_activities(cli, start_date.isoformat(), end_date.isoformat())
        ids = [activity_id(x) for x in summaries]
        if any(x is None for x in ids) or len(ids) != len(set(ids)):
            raise RuntimeError(f"YEAR {year}: Garmin index contains missing or duplicate activity IDs")

        if year >= 2026:
            by_month: dict[str, list[dict[str, Any]]] = {}
            for summary in summaries:
                by_month.setdefault(month_of_activity({"summary": summary}), []).append(summary)
            for month, month_summaries in sorted(by_month.items()):
                if verify_monthly_history_file(dbx, month, month_summaries):
                    continue
                month_rows = activity_records(cli, month_summaries)
                got = [activity_id(x.get("summary", {})) for x in month_rows]
                expected = [activity_id(x) for x in month_summaries]
                if len(got) != len(expected) or None in got or len(got) != len(set(got)) or set(got) != set(expected):
                    raise RuntimeError(f"MONTH {month}: incomplete details; file not published")
                errors: list[dict[str, str]] = []
                p_ok, profile = optional(cli, errors, f"profile-settings {month}", "profile-settings")
                z_ok, zones = optional(cli, errors, f"heart-rate-zones {month}", "heart-rate-zones")
                save_monthly_history_file(dbx, month, month_rows,
                                          profile if p_ok else None, zones if z_ok else None)
            continue

        if verify_existing_annual_file(dbx, year, summaries):
            skipped_years.append(year)
            continue
        if not summaries:
            LOG.info("YEAR %d: no activities; no archive file required", year)
            continue

        LOG.info("YEAR %d: downloading details for %d activities",
                 year, len(summaries))
        records = activity_records(cli, summaries)
        record_ids = [activity_id(x.get("summary", {})) for x in records]
        if (len(records) != len(summaries) or None in record_ids
                or len(record_ids) != len(set(record_ids))
                or set(record_ids) != set(ids)):
            raise RuntimeError(f"YEAR {year}: detail export incomplete; annual file not published")

        errors: list[dict[str, str]] = []
        profile_ok, profile = optional(cli, errors, f"profile-settings {year}", "profile-settings")
        zones_ok, zones = optional(cli, errors, f"heart-rate-zones {year}", "heart-rate-zones")
        if not profile_ok or not zones_ok:
            LOG.warning("YEAR %d: profile/zone metadata partially unavailable; activity data is complete",
                        year)
        save_annual_file(dbx, year, records,
                         profile if profile_ok else None,
                         zones if zones_ok else None)
        archived_years.append(year)
        LOG.info("YEAR %d: published and verified (%d activities)", year, len(records))

    # Current year is deliberately separate and contains year-to-date activities.
    LOG.info("CURRENT %d: fetching year-to-date activity index", current_year)
    current_summaries = fetch_activities(
        cli, date(current_year, 1, 1).isoformat(), today.isoformat())
    current_ids = [activity_id(x) for x in current_summaries]
    if any(x is None for x in current_ids) or len(current_ids) != len(set(current_ids)):
        raise RuntimeError("CURRENT: Garmin index contains missing or duplicate activity IDs")
    LOG.info("CURRENT %d: downloading details for %d activities",
             current_year, len(current_summaries))
    current_rows = activity_records(cli, current_summaries)
    row_ids = [activity_id(x.get("summary", {})) for x in current_rows]
    if (len(current_rows) != len(current_summaries) or None in row_ids
            or len(row_ids) != len(set(row_ids)) or set(row_ids) != set(current_ids)):
        raise RuntimeError("CURRENT: activity detail export incomplete; refusing upload")

    current = build_current(cli, today, None, current_rows)
    uploaded_months: list[str] = []
    for month, month_rows in group_by_month(current_rows).items():
        month_start, month_end = month_bounds(month)
        monthly = dict(current)
        monthly["activities"] = month_rows
        monthly["period"] = {"start_date": month_start,
                             "end_date": min(date.fromisoformat(month_end), today).isoformat()}
        monthly["metadata"] = dict(current["metadata"])
        monthly["metadata"].update({"file_role": "rolling_current_month",
                                    "month": month, "activity_count": len(month_rows)})
        path = f"{CURRENT_DIR}/garmin_{month}.json"
        upload_json(dbx, path, monthly)
        uploaded_months.append(month)

    LOG.info("Finished. Newly archived years: %s; verified/skipped years: %s; "
             "current year %d monthly files: %s; activities: %d; Garmin requests: %d",
             ", ".join(map(str, archived_years)) or "none",
             ", ".join(map(str, skipped_years)) or "none",
             current_year, ", ".join(uploaded_months) or "none",
             len(current_rows), cli.request_count)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOG.exception("Garmin export failed.")
        raise
