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
from garmin_personal_api.api_keys import resolve_api_key
from garmin_personal_api.auth import PersonalApiAuthenticator
from garmin_personal_api.client import GarminPersonalApiClient

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
# Full-history export: date windows are determined by archive periods, never rolling limits.
CURRENT_ACTIVITY_DAYS = 0
RANGE_DAYS = 0
DAILY_DETAIL_DAYS = 0
REQUESTS_BETWEEN_PAUSES = 50

# Device IDs confirmed against the user's Garmin export. Unknown IDs remain null.
DEVICE_MODEL_OVERRIDES = {
    "3819067544": {"device_model": "Garmin Forerunner 305"},
    "3934164543": {"device_model": "Garmin fēnix 3 HR"},
    "3359313181": {
        "device_model": "Garmin fēnix 6 Sapphire",
        "part_number": "010-02158-11",
        "model_source": "user_confirmed_purchase",
    },
    "3503792255": {"device_model": "Garmin epix Pro (Gen 2), 47 mm Sapphire"},
}


GEAR_REGISTRY: dict[int, dict[str, Any]] = {}


def fetch_gear_data(cli: GarminCLI) -> tuple[dict[int, dict[str, Any]], dict[str, int]]:
    """Fetch Garmin gear and gear-to-activity links through read-only Personal API endpoints."""
    profile_ok, profile = optional(cli, [], "profile-settings for gear", "profile-settings")
    if not profile_ok or not isinstance(profile, dict):
        raise RuntimeError("Cannot retrieve profile settings required for gear export")
    profile_id = profile.get("userProfilePk") or profile.get("userProfileId")
    if profile_id is None:
        for value in profile.values():
            if isinstance(value, dict):
                profile_id = value.get("userProfilePk") or value.get("userProfileId")
                if profile_id is not None:
                    break
    if profile_id is None:
        # Stable profile key confirmed in the user's Garmin Connect export.
        profile_id = 3889253

    api_key = resolve_api_key()
    with PersonalApiAuthenticator() as auth:
        token = auth.obtain_token(api_key)
        with GarminPersonalApiClient(token.access_token) as client:
            gear_response = client.get(
                "/gear-service/gear/filterGear",
                params={"userProfilePk": str(profile_id)},
            )

            # Garmin returns a wrapper containing both the gear registry and
            # the activity links. The export format is typically a one-item
            # list: [{"gearDTOS": [...], "gearActivityDTOs": {...}}].
            payload = gear_response
            if isinstance(payload, list) and len(payload) == 1 and isinstance(payload[0], dict):
                payload = payload[0]
            if isinstance(payload, dict):
                gear_rows = payload.get("gearDTOS", [])
                gear_activity_rows = payload.get("gearActivityDTOs", {})
            elif isinstance(payload, list):
                # Keep compatibility with responses containing gear rows only.
                gear_rows = payload
                gear_activity_rows = {}
            else:
                raise RuntimeError("Garmin gear endpoint returned an unexpected response")

            if not isinstance(gear_rows, list) or not isinstance(gear_activity_rows, dict):
                raise RuntimeError("Garmin gear endpoint returned an invalid gear payload")

            registry: dict[int, dict[str, Any]] = {}
            activity_to_gear: dict[str, int] = {}
            for item in gear_rows:
                if not isinstance(item, dict) or item.get("gearPk") is None:
                    continue
                gear_id = int(item["gearPk"])
                registry[gear_id] = item

            # gearActivityDTOs is keyed by gearPk and contains activityId rows.
            # Do not call the old UUID-based endpoint: it returned 404 and is
            # unnecessary when Garmin supplies the complete mapping here.
            for gear_key, links in gear_activity_rows.items():
                if not isinstance(links, list):
                    continue
                try:
                    gear_id = int(gear_key)
                except (TypeError, ValueError):
                    continue
                for link in links:
                    if not isinstance(link, dict) or link.get("activityId") is None:
                        continue
                    link_gear_id = link.get("gearPk", gear_id)
                    try:
                        link_gear_id = int(link_gear_id)
                    except (TypeError, ValueError):
                        continue
                    activity_to_gear[str(link["activityId"])] = link_gear_id
    LOG.info("Fetched %d gear items and %d activity-to-gear links",
             len(registry), len(activity_to_gear))
    return registry, activity_to_gear


def enrich_gear_records(rows: list[dict[str, Any]],
                        registry: dict[int, dict[str, Any]],
                        activity_to_gear: dict[str, int]) -> None:
    for row in rows:
        summary = row.get("summary", {})
        aid = activity_id(summary) if isinstance(summary, dict) else None
        gear_id = activity_to_gear.get(aid) if aid else None
        row["gear"] = (
            {"gear_id": gear_id, "gear": registry.get(gear_id)}
            if gear_id is not None else None
        )



def enrich_device_ids(value: Any) -> Any:
    """Add a resolved device model wherever a Garmin payload includes deviceId."""
    if isinstance(value, list):
        return [enrich_device_ids(item) for item in value]
    if isinstance(value, dict):
        result = {key: enrich_device_ids(item) for key, item in value.items()}
        device_id = result.get("deviceId", result.get("device_id"))
        if device_id is not None:
            device_id = str(device_id)
            result["device_id"] = device_id
            result["device_model"] = DEVICE_MODEL_OVERRIDES.get(device_id, {}).get("device_model")
        return result
    return value


def gear_for_activity(activity: dict[str, Any]) -> dict[str, Any] | None:
    """Resolve gear when the activity payload contains a Garmin gear primary key."""
    for source in (activity.get("detail"), activity.get("summary"), activity):
        if isinstance(source, dict):
            gear_id = source.get("gearPk", source.get("gearId", source.get("gear_id")))
            if gear_id is not None:
                try:
                    key = int(gear_id)
                except (TypeError, ValueError):
                    continue
                meta = GEAR_REGISTRY.get(key)
                return {"gear_id": key, **(meta or {"name": None, "type": None})}
    return None

def device_info_for_activity(row: dict[str, Any]) -> dict[str, Any] | None:
    """Return the device identity attached to an activity, when Garmin supplies it."""
    for section in ("summary", "detail"):
        value = row.get(section)
        if not isinstance(value, dict):
            continue
        device_id = value.get("deviceId", value.get("device_id"))
        if device_id is None:
            continue
        device_id = str(device_id)
        override = DEVICE_MODEL_OVERRIDES.get(device_id, {})
        return {
            "device_id": device_id,
            "device_model": override.get("device_model"),
            "part_number": override.get("part_number"),
            "model_source": override.get("model_source", "garmin_device_registry"),
        }
    return None


def device_registry(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build a deduplicated registry from device IDs present in exported activities."""
    devices: dict[str, dict[str, Any]] = {}
    for row in rows:
        info = device_info_for_activity(row)
        if info:
            devices[info["device_id"]] = info
    for device_id, override in DEVICE_MODEL_OVERRIDES.items():
        if device_id in devices:
            devices[device_id].update(override)
    return [devices[key] for key in sorted(devices)]


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
        message = str(exc)
        # Garmin may return 404 when a date has no recorded value. Treat that
        # as an ordinary gap; genuine transport/auth/API failures remain errors.
        if re.search(r"\b404\b|no data available|no data for (?:this )?(?:date|period)", message, re.I):
            LOG.info("%s: no data available", label)
            return True, None
        LOG.error("%s failed: %s", label, exc)
        errors.append({"source": label, "error": message})
        return False, None


def optional_range(cli: GarminCLI, errors: list[dict[str, str]],
                  unavailable: list[dict[str, str]], label: str, *args: str
                  ) -> tuple[bool, Any]:
    """Fetch a documented date-range source; record Garmin 400 empty-history responses."""
    try:
        return True, cli.run(label, *args)
    except Exception as exc:
        message = str(exc)
        if re.search(r"HTTP\s*400", message, re.I):
            # Garmin returns HTTP 400 with an empty body for these daily-stat
            # endpoints when the requested historical period has no records
            # (for example, step/stress history predating compatible devices).
            # Keep this distinct from measured zero and preserve the reason.
            LOG.warning("%s unavailable for requested period: Garmin returned HTTP 400", label)
            unavailable.append({"source": label, "reason": "Garmin HTTP 400; no records returned"})
            return True, None
        if re.search(r"\b404\b|no data available|no data for (?:this )?(?:date|period)", message, re.I):
            LOG.info("%s: no data available", label)
            return True, None
        LOG.error("%s failed: %s", label, exc)
        errors.append({"source": label, "error": message})
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
                record = {"summary": summary, "detail": cli.run(f"activity {aid}", "activity", aid),
                          "workouts": cli.run(f"activity-workouts {aid}", "activity-workouts", aid)}
                record = enrich_device_ids(record)
                device = device_info_for_activity(record)
                if device:
                    record["device"] = device
                records.append(record)
                break
            except Exception as exc:
                error = exc
                LOG.warning("Activity %s failed attempt %d/3: %s", aid, attempt, exc)
                if attempt < 3: time.sleep(5 * attempt)
        else: raise RuntimeError(f"Activity {aid} failed after 3 attempts: {error}")
    return records
def build_current(cli: GarminCLI, today: date, old_current: dict[str, Any] | None,
                  activity_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    # Current-year data is exported from January 1 through today, not a 14/30-day window.
    activity_start = date(today.year, 1, 1)
    range_start = activity_start
    daily_start = activity_start
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
            "activity_window_days": None,
            "range_window_days": None,
            "daily_detail_window_days": None,
            "history_scope": "all_available_current_year",
        },
        "period": {"start_date": activity_start.isoformat(), "end_date": today.isoformat()},
        "profile": None,
        "heart_rate_zones": None,
        "source_status": {},
        "source_errors": errors,
        "devices": device_registry(activities),
        "gear_registry": [{"gear_id": key, **value}
                          for key, value in (gear_registry or GEAR_REGISTRY).items()],
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
        "stress": "stress", "steps": "steps",
        "weight_measurements": "weight-measurements",
        "weight_goals": "effective-weight-goals",
        "sleep_summaries": "sleep-summaries",
        "body_battery": "body-battery", "daily_summaries": "daily-summaries",
        "hrv_summaries": "hrv-summaries",
        "wellness_activity_summaries": "wellness-activity-summaries",
    }.items():
        ok, value = optional(cli, errors, command, command, "--start-date",
                             range_start.isoformat(), "--end-date", today.isoformat())
        current["daily"]["range_data"][field] = (
            enrich_device_ids(date_index(value)) if ok else {"raw": None, "by_date": {}})
        current["source_status"][command] = "ok" if ok else "error"
    for offset in range((today - daily_start).days + 1):
        d = (daily_start + timedelta(days=offset)).isoformat()
        current["daily"]["by_date"][d] = {}
        for field, command in (("sleep", "sleep"), ("heart_rate", "heart-rate"),
                               ("hrv", "hrv"), ("spo2", "spo2"),
                               ("fitness_age", "fitness-age")):
            ok, value = optional(cli, errors, f"{command} {d}", command, "--date", d)
            # A successful empty response is a normal data gap, not an error.
            current["daily"]["by_date"][d][field] = enrich_device_ids(value) if ok else None
            current["source_status"][f"{command} {d}"] = (
                "ok" if ok and value is not None else
                "no_data" if ok else "error")
    current["metadata"]["garmin_api_requests"] = cli.request_count
    current["metadata"]["activity_count"] = len(activities)
    current["metadata"]["source_error_count"] = len(errors)
    current["metadata"]["export_status"] = "complete" if not errors else "partial"
    return current


def ensure_dropbox_folders(dbx: dropbox.Dropbox) -> None:
    """Ensure the app-folder archive destinations exist."""
    for path in (HISTORY_DIR, CURRENT_DIR):
        try:
            metadata = dbx.files_get_metadata(path)
        except dropbox.exceptions.ApiError as exc:
            if exc.error.is_path() and exc.error.get_path().is_not_found():
                dbx.files_create_folder_v2(path)
                LOG.info("Created missing Dropbox folder %s", path)
                continue
            raise
        if not isinstance(metadata, dropbox.files.FolderMetadata):
            raise RuntimeError(f"Expected a Dropbox folder at {path}")


def optional_daily(cli: GarminCLI, unavailable: list[dict[str, str]],
                  label: str, *args: str) -> tuple[bool, Any]:
    """Retry transient daily-endpoint failures three times, then record a gap and continue."""
    for attempt in range(1, 4):
        try:
            return True, cli.run(label, *args)
        except Exception as exc:
            message = str(exc)
            if re.search(r"HTTP\\s*5\\d\\d|timed out|timeout|temporar", message, re.I):
                if attempt < 3:
                    wait = 5 * attempt
                    LOG.warning("%s failed attempt %d/3; retrying in %d sec: %s",
                                label, attempt, wait, message)
                    time.sleep(wait)
                    continue
                LOG.warning("%s failed after 3 attempts; recording unavailable and continuing: %s",
                            label, message)
                unavailable.append({"source": label, "reason": "failed after 3 attempts: " + message})
                return True, None
            if re.search(r"\\b404\\b|no data available|no data for (?:this )?(?:date|period)", message, re.I):
                LOG.info("%s: no data available", label)
                return True, None
            raise


def fetch_month_health(cli: GarminCLI, month: str) -> dict[str, Any]:
    """Fetch all supported date-range health sources for one historical month."""
    start, end = month_bounds(month)
    errors: list[dict[str, str]] = []
    unavailable: list[dict[str, str]] = []
    data: dict[str, Any] = {"range_data": {}, "source_errors": errors,
                            "unavailable_sources": unavailable}
    sources = {
        "stress": "stress",
        "steps": "steps",
        "weight_measurements": "weight-measurements",
        "weight_goals": "effective-weight-goals",
        "sleep_summaries": "sleep-summaries",
        "body_battery": "body-battery",
        "daily_summaries": "daily-summaries",
        "hrv_summaries": "hrv-summaries",
        "wellness_activity_summaries": "wellness-activity-summaries",
    }
    for field, command in sources.items():
        label = f"{command} {month}"
        args = (command, "--start-date", start, "--end-date", end)
        if field in ("stress", "steps"):
            ok, value = optional_range(cli, errors, unavailable, label, *args)
        else:
            ok, value = optional(cli, errors, label, *args)
        data["range_data"][field] = (
            enrich_device_ids(date_index(value)) if ok and value is not None
            else {"raw": None, "by_date": {},
                  "availability": "unavailable" if any(
                      item["source"] == label for item in unavailable) else "no_data"})
    # Daily endpoints must be queried per date. Fetch every calendar day in the
    # archive month; an empty response is a normal data gap, not an export failure.
    data["by_date"] = {}
    first = date.fromisoformat(start)
    last = min(date.fromisoformat(end), datetime.now(TZ).date())
    for offset in range((last - first).days + 1):
        day = (first + timedelta(days=offset)).isoformat()
        daily_row: dict[str, Any] = {}
        for field, command in (("sleep", "sleep"), ("heart_rate", "heart-rate"),
                               ("hrv", "hrv"), ("spo2", "spo2")):
            ok, value = optional_daily(cli, unavailable, f"{command} {day}",
                                       command, "--date", day)
            daily_row[field] = enrich_device_ids(value) if ok else None
        data["by_date"][day] = daily_row
    data["metadata"] = {
        "month": month, "source_error_count": len(errors),
        "unavailable_source_count": len(unavailable),
        "export_status": "complete" if not errors else "partial",
        "daily_detail_dates_queried": len(data["by_date"]),
        "history_scope": "full_month_all_available_dates",
    }
    return data


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
        "devices": device_registry(rows),
        "gear_registry": list({
            item["gear"]["gear_id"]: {
                "gear_id": item["gear"]["gear_id"],
                **(item["gear"].get("gear") or {}),
            }
            for item in rows if isinstance(item.get("gear"), dict)
        }.values()),
        "activities": rows,
    }


def monthly_payload(month: str, rows: list[dict[str, Any]], profile: Any,
                    zones: Any, file_role: str,
                    health: dict[str, Any] | None = None,
                    gear_registry: dict[int, dict[str, Any]] | None = None) -> dict[str, Any]:
    ids = [activity_id(x.get("summary", {})) for x in rows]
    if None in ids or len(ids) != len(set(ids)):
        raise RuntimeError(f"Invalid or duplicate activity IDs in {month}")
    start, end = month_bounds(month)
    return {
        "schema_version": 1,
        "metadata": {"file_role": file_role, "created_at": datetime.now(timezone.utc).isoformat(),
                     "activity_count": len(rows),
                     "export_status": (health or {}).get("metadata", {}).get("export_status", "complete"),
                     "source": "Garmin Connect", "month": month},
        "period": {"start_date": start, "end_date": end},
        "profile": profile, "heart_rate_zones": zones,
        "devices": device_registry(rows),
        "gear_registry": [{"gear_id": key, **value}
                          for key, value in (gear_registry or GEAR_REGISTRY).items()],
        "activities": rows,
        "health": health or {},
    }


def verify_monthly_history_file(dbx: dropbox.Dropbox, month: str,
                                summaries: list[dict[str, Any]]) -> bool:
    path = f"{HISTORY_DIR}/garmin_history_{month}.json"
    if not dropbox_file_exists(dbx, path):
        return False
    existing = read_dropbox(dbx, path)
    if not existing:
        return False
    status = existing.get("metadata", {}).get("export_status")
    health = existing.get("health", {})
    health_meta = health.get("metadata", {}) if isinstance(health, dict) else {}
    if (not isinstance(health, dict)
            or not isinstance(health.get("by_date"), dict)
            or health_meta.get("history_scope") != "full_month_all_available_dates"):
        LOG.info("Existing %s lacks full-history daily health; rebuilding month", path)
        return False
    if status == "partial":
        LOG.info("Existing %s is partial; retrying month", path)
        return False
    if status != "complete":
        raise RuntimeError(f"Existing {path} has unknown export status; refusing overwrite")
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
                              rows: list[dict[str, Any]], profile: Any, zones: Any,
                              health: dict[str, Any] | None = None,
                              gear_registry: dict[int, dict[str, Any]] | None = None,
                              force_refresh: bool = False) -> None:
    path = f"{HISTORY_DIR}/garmin_history_{month}.json"
    payload = monthly_payload(month, rows, profile, zones,
                              "immutable_activity_history_monthly", health, gear_registry)
    mode = WriteMode.add
    if dropbox_file_exists(dbx, path):
        existing = read_dropbox(dbx, path) or {}
        old_ids = {activity_id(x.get("summary", {})) for x in existing.get("activities", [])
                   if isinstance(x, dict)}
        if old_ids != {activity_id(x.get("summary", {})) for x in rows}:
            raise RuntimeError(f"Existing {path} differs from Garmin; refusing overwrite")
        old_health = existing.get("health", {})
        old_health_meta = old_health.get("metadata", {}) if isinstance(old_health, dict) else {}
        if (not force_refresh
                and existing.get("metadata", {}).get("export_status") == "complete"
                and isinstance(old_health, dict)
                and isinstance(old_health.get("by_date"), dict)
                and old_health_meta.get("history_scope") == "full_month_all_available_dates"):
            return
        mode = WriteMode.overwrite
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    dbx.files_upload(data, path, mode=mode, mute=True)
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
    current_month = today.strftime("%Y-%m")
    cli = GarminCLI()
    # Gear lookup is currently an optional enrichment: Garmin's previously used
    # Personal API endpoint returns 404. Do not let this secondary source block
    # the full activity and health-history export.
    try:
        gear_registry, activity_to_gear = fetch_gear_data(cli)
    except Exception as exc:
        LOG.warning("Gear registry unavailable; continuing without gear links: %s", exc)
        gear_registry, activity_to_gear = {}, {}
    GEAR_REGISTRY.update(gear_registry)

    # Collect every activity index year first, then build a continuous month
    # calendar from the earliest activity through the current month. Months
    # without activities are still exported because health data exists without
    # a recorded workout.
    summaries_by_month: dict[str, list[dict[str, Any]]] = {}
    for year in range(2000, today.year + 1):
        year_start = date(year, 1, 1)
        year_end = min(date(year, 12, 31), today)
        summaries = fetch_activities(cli, year_start.isoformat(), year_end.isoformat())
        ids = [activity_id(row) for row in summaries]
        if any(value is None for value in ids) or len(ids) != len(set(ids)):
            raise RuntimeError(f"YEAR {year}: Garmin index contains missing or duplicate activity IDs")
        for summary in summaries:
            month = month_of_activity({"summary": summary})
            summaries_by_month.setdefault(month, []).append(summary)

    if not summaries_by_month:
        raise RuntimeError("Garmin returned no activities; cannot determine the start of available history")
    first_month = min(summaries_by_month)
    first_date = date.fromisoformat(first_month + "-01")
    months: list[str] = []
    cursor = first_date
    while cursor <= today.replace(day=1):
        months.append(cursor.strftime("%Y-%m"))
        cursor = date(cursor.year + (cursor.month == 12),
                      cursor.month % 12 + 1, 1)

    completed: list[str] = []
    rebuilt: list[str] = []
    # Refresh activity details for exactly the latest 30 calendar days,
    # independent of the current day of the month.
    refresh_start = today - timedelta(days=29)
    for month in months:
        month_summaries = summaries_by_month.get(month, [])
        is_current = month == current_month
        path = (f"{CURRENT_DIR}/garmin_{month}.json" if is_current
                else f"{HISTORY_DIR}/garmin_history_{month}.json")
        recent_summaries = [
            summary for summary in month_summaries
            if activity_datetime({"summary": summary}).date() >= refresh_start
        ]

        # Months entirely outside the rolling window remain untouched.
        if not is_current and not recent_summaries:
            if verify_monthly_history_file(dbx, month, month_summaries):
                completed.append(month)
                continue

        existing = read_dropbox(dbx, path) if dropbox_file_exists(dbx, path) else None
        existing_rows = existing.get("activities") if isinstance(existing, dict) else None
        existing_ids = [
            activity_id(row.get("summary", {}))
            for row in existing_rows if isinstance(row, dict)
        ] if isinstance(existing_rows, list) else []
        expected_ids = [activity_id(summary) for summary in month_summaries]
        existing_valid = (
            isinstance(existing, dict)
            and existing.get("metadata", {}).get("export_status") == "complete"
            and isinstance(existing_rows, list)
            and len(existing_ids) == len(existing_rows)
            and None not in existing_ids
            and len(existing_ids) == len(set(existing_ids))
            and set(existing_ids) == set(expected_ids)
        )
        if existing_valid and not is_current:
            old_health = existing.get("health", {})
            old_health_meta = old_health.get("metadata", {}) if isinstance(old_health, dict) else {}
            existing_valid = (
                isinstance(old_health, dict)
                and isinstance(old_health.get("by_date"), dict)
                and old_health_meta.get("history_scope") == "full_month_all_available_dates"
            )

        if existing_valid:
            # Re-fetch only activity details inside the rolling 30-day window.
            refreshed_rows = activity_records(cli, recent_summaries) if recent_summaries else []
            enrich_gear_records(refreshed_rows, gear_registry, activity_to_gear)
            refreshed_ids = [activity_id(row.get("summary", {})) for row in refreshed_rows]
            expected_recent_ids = [activity_id(summary) for summary in recent_summaries]
            if (len(refreshed_ids) != len(expected_recent_ids)
                    or None in refreshed_ids
                    or len(refreshed_ids) != len(set(refreshed_ids))
                    or set(refreshed_ids) != set(expected_recent_ids)):
                raise RuntimeError(
                    f"MONTH {month}: incomplete refreshed activity details; file not published"
                )

            rows_by_id = {
                activity_id(row.get("summary", {})): row for row in existing_rows
            }
            for row in refreshed_rows:
                rows_by_id[activity_id(row.get("summary", {}))] = row
            month_rows = sorted(rows_by_id.values(), key=activity_datetime)

            if not is_current:
                # Keep the already complete monthly health history unchanged.
                save_monthly_history_file(
                    dbx, month, month_rows, existing.get("profile"),
                    existing.get("heart_rate_zones"), existing.get("health"),
                    gear_registry, force_refresh=True,
                )
                completed.append(month)
                LOG.info(
                    "MONTH %s: refreshed %d activities within rolling 30-day window; "
                    "preserved %d older activities",
                    month, len(refreshed_rows), len(month_rows) - len(refreshed_rows),
                )
                continue
        else:
            # Missing/incomplete files retain the original full-month recovery path.
            month_rows = activity_records(cli, month_summaries) if month_summaries else []
            enrich_gear_records(month_rows, gear_registry, activity_to_gear)

        got = [activity_id(row.get("summary", {})) for row in month_rows]
        expected = [activity_id(row) for row in month_summaries]
        if (len(got) != len(expected) or None in got or len(got) != len(set(got))
                or set(got) != set(expected)):
            raise RuntimeError(f"MONTH {month}: incomplete activity details; file not published")

        errors: list[dict[str, str]] = []
        p_ok, profile = optional(cli, errors, f"profile-settings {month}", "profile-settings")
        z_ok, zones = optional(cli, errors, f"heart-rate-zones {month}", "heart-rate-zones")
        health = fetch_month_health(cli, month)
        month_errors = errors + health.get("source_errors", [])
        health.setdefault("metadata", {})["export_status"] = (
            "complete" if not month_errors else "partial")
        health["metadata"]["source_error_count"] = len(month_errors)

        payload = monthly_payload(
            month, month_rows, profile if p_ok else None, zones if z_ok else None,
            "current_month" if is_current else "immutable_activity_history_monthly",
            health, gear_registry)
        payload["metadata"]["export_status"] = (
            "complete" if not month_errors else "partial")
        payload["metadata"]["source_error_count"] = len(month_errors)
        payload["metadata"]["history_scope"] = "full_month_all_available_dates"
        payload["metadata"]["gear_link_count"] = sum(
            1 for row in month_rows if isinstance(row.get("gear"), dict))
        if is_current:
            upload_json(dbx, path, payload)
        else:
            save_monthly_history_file(dbx, month, month_rows,
                                      profile if p_ok else None,
                                      zones if z_ok else None, health, gear_registry)
        if month_errors:
            raise RuntimeError(
                f"MONTH {month}: export is partial ({len(month_errors)} source errors); "
                "fix the source/API issue and rerun to resume safely")
        completed.append(month)
        LOG.info("MONTH %s: complete (%d activities, %d daily dates)",
                 month, len(month_rows), len(health.get("by_date", {})))

    LOG.info("Finished full-history export. Months complete: %d (%s to %s); "
             "activities: %d; gear links: %d; Garmin requests: %d",
             len(completed), first_month, current_month,
             sum(len(rows) for rows in summaries_by_month.values()),
             len(activity_to_gear), cli.request_count)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOG.exception("Garmin export failed.")
        raise
