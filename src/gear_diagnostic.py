#!/usr/bin/env python3
"""Read-only one-month Garmin gear endpoint diagnostic. No Dropbox access or writes."""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date
from typing import Any

import requests
from urllib.parse import urlencode

from garmin_personal_api.api_keys import resolve_api_key
from garmin_personal_api.auth import PersonalApiAuthenticator
from garmin_personal_api.client import GarminPersonalApiClient

from garmin_export import GarminCLI, fetch_activities

LOG = logging.getLogger("gear_diagnostic")


def profile_id_from(value: Any) -> tuple[str | None, str | None]:
    if isinstance(value, dict):
        for key in ("userProfilePk", "userProfileId"):
            if value.get(key) is not None:
                return str(value[key]), key
        for nested in value.values():
            found, source = profile_id_from(nested)
            if found:
                return found, source
    elif isinstance(value, list):
        for nested in value:
            found, source = profile_id_from(nested)
            if found:
                return found, source
    return None, None


def summarize(label: str, payload: Any) -> None:
    if isinstance(payload, list):
        sample = payload[0] if payload else None
        LOG.info("%s: HTTP OK; type=list; count=%d; first_item_keys=%s",
                 label, len(payload), sorted(sample.keys()) if isinstance(sample, dict) else None)
        if isinstance(sample, dict):
            LOG.info("%s sample_fields=%s", label, json.dumps(
                {k: sample.get(k) for k in (
                    "gearPk", "uuid", "gearUUID", "name", "customMakeModel",
                    "gearType", "activityId", "activityType"
                ) if k in sample}, ensure_ascii=False, default=str))
    elif isinstance(payload, dict):
        LOG.info("%s: HTTP OK; type=dict; keys=%s", label, sorted(payload.keys()))
        for key in ("gear", "gearList", "items", "data", "activities", "activityIds"):
            value = payload.get(key)
            if isinstance(value, list):
                LOG.info("%s.%s count=%d", label, key, len(value))
    else:
        LOG.info("%s: HTTP OK; type=%s; value=%s",
                 label, type(payload).__name__, str(payload)[:300])


def try_get(client: GarminPersonalApiClient, label: str, path: str,
            params: dict[str, Any] | None = None) -> Any | None:
    suffix = "?" + urlencode(params) if params else ""
    try:
        payload = client.get(path, params=params or {})
        summarize(label, payload)
        return payload
    except Exception as exc:
        LOG.warning("%s FAILED: %s", label, str(exc)[:500])
        return None


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    month = os.getenv("GEAR_TEST_MONTH", "2026-10")
    first = date.fromisoformat(month + "-01")
    if first.month == 12:
        next_month = date(first.year + 1, 1, 1)
    else:
        next_month = date(first.year, first.month + 1, 1)
    start = first.isoformat()
    end = (next_month - __import__("datetime").timedelta(days=1)).isoformat()
    LOG.info("Read-only gear diagnostic for %s (%s..%s)", month, start, end)

    cli = GarminCLI()
    profile_ok, profile = True, cli.run("profile-settings", "profile-settings")
    profile_id, profile_source = profile_id_from(profile)
    if not profile_id:
        profile_id = "3889253"
        profile_source = "existing-script fallback"
    LOG.info("Profile identifier selected: %s (field/source: %s)", profile_id, profile_source)

    activities = fetch_activities(cli, start, end)
    activity_ids = [str(row.get("activityId", row.get("activity_id", row.get("id"))))
                    for row in activities]
    LOG.info("Month activity index: %d activities", len(activity_ids))
    if not activity_ids:
        LOG.warning("No activities in selected month; gear endpoint probes will still run.")

    api_key = resolve_api_key()
    with PersonalApiAuthenticator() as auth:
        token = auth.obtain_token(api_key)

        # The Personal API host returned 404 for every gear route. Probe Garmin's
        # regular Connect hosts with the same bearer token, read-only, before
        # concluding that gear requires a different authentication flow.
        headers = {
            "Authorization": f"Bearer {token.access_token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://connect.garmin.com",
            "Referer": "https://connect.garmin.com/modern/",
            "DI-Backend": "connectapi.garmin.com",
        }
        probes = [
            ("legacy gear list", "/gear-service/gear/filterGear",
             {"userProfilePk": profile_id}),
            ("v2 gear list", "/gear-service/gear/v2/list",
             {"start": 0, "limit": 100}),
        ]
        for host in ("https://connect.garmin.com", "https://connectapi.garmin.com"):
            for label, path, params in probes:
                try:
                    response = requests.get(
                        host + path, headers=headers, params=params,
                        timeout=20, allow_redirects=False)
                    LOG.info("HOST PROBE %s %s: HTTP %d; content_type=%s; body=%s",
                             host, label, response.status_code,
                             response.headers.get("Content-Type"),
                             response.text[:350].replace("\\n", " "))
                except Exception as exc:
                    LOG.warning("HOST PROBE %s %s FAILED: %s", host, label, str(exc)[:300])

        with GarminPersonalApiClient(token.access_token) as client:
            legacy_gears = try_get(
                client, "gear list legacy filterGear",
                "/gear-service/gear/filterGear", {"userProfilePk": profile_id})
            v2_gears = try_get(
                client, "gear list v2",
                "/gear-service/gear/v2/list", {"start": 0, "limit": 100})

            # Test both activity-specific lookup variants against this month's IDs.
            legacy_linked: dict[str, Any] = {}
            v2_linked: dict[str, Any] = {}
            for index, activity_id in enumerate(activity_ids, 1):
                legacy = try_get(
                    client, f"legacy gear by activity {activity_id}",
                    "/gear-service/gear/filterGear", {"activityId": activity_id})
                newer = try_get(
                    client, f"v2 gear by activity {activity_id}",
                    f"/gear-service/activity/v2/{activity_id}")
                if legacy:
                    legacy_linked[activity_id] = legacy
                if newer:
                    v2_linked[activity_id] = newer
                if index % 10 == 0:
                    LOG.info("Checked gear association endpoints for %d/%d activities",
                             index, len(activity_ids))
                time.sleep(0.25)

            # Also test the documented reverse lookup per registered gear.
            gear_rows = legacy_gears if isinstance(legacy_gears, list) else []
            if not gear_rows and isinstance(v2_gears, dict):
                for key in ("gear", "gearList", "items", "data"):
                    if isinstance(v2_gears.get(key), list):
                        gear_rows = v2_gears[key]
                        break
            reverse_matches: dict[str, int] = {}
            for gear in gear_rows:
                if not isinstance(gear, dict):
                    continue
                gear_uuid = gear.get("uuid") or gear.get("gearUUID")
                if not gear_uuid:
                    continue
                result = try_get(
                    client, f"activities for gear {str(gear_uuid)[:12]}",
                    f"/activitylist-service/activities/{gear_uuid}/gear",
                    {"start": 0, "limit": 1000})
                if isinstance(result, list):
                    ids = {
                        str(item.get("activityId", item.get("activity_id", item.get("id"))))
                        for item in result if isinstance(item, dict)
                    }
                    reverse_matches[str(gear_uuid)] = len(ids.intersection(activity_ids))
                time.sleep(0.25)

    LOG.info("========== DIAGNOSTIC SUMMARY ==========")
    LOG.info("month=%s activities=%d", month, len(activity_ids))
    LOG.info("legacy_gear_list=%s", "OK" if isinstance(legacy_gears, list) else "FAILED/UNEXPECTED")
    LOG.info("v2_gear_list=%s", "OK" if v2_gears is not None else "FAILED")
    LOG.info("legacy_activity_gear_successes=%d/%d", len(legacy_linked), len(activity_ids))
    LOG.info("v2_activity_gear_successes=%d/%d", len(v2_linked), len(activity_ids))
    LOG.info("reverse_gear_lookup_matches=%s", json.dumps(reverse_matches, ensure_ascii=False))
    LOG.info("No Dropbox client was created; no files were changed.")


if __name__ == "__main__":
    main()
