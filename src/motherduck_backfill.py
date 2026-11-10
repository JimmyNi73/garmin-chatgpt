"""Full idempotent backfill from the authoritative monthly Garmin Dropbox exports."""
from __future__ import annotations

import json
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from garmin_export import CURRENT_DIR, HISTORY_DIR, dbx_client
from motherduck_loader import connect, ensure_schema, load_records

LOG = logging.getLogger("motherduck_backfill")


def _activity_id(row: dict[str, Any]) -> str | None:
    summary = row.get("summary") if isinstance(row.get("summary"), dict) else row
    value = summary.get("activityId", summary.get("activity_id", summary.get("id")))
    return str(value) if value is not None else None


def _append_health(datasets: dict[str, list[dict[str, Any]]],
                   health: dict[str, Any]) -> None:
    range_data = health.get("range_data", {})
    if isinstance(range_data, dict):
        for metric, indexed in range_data.items():
            if not isinstance(indexed, dict):
                continue
            by_date = indexed.get("by_date", {})
            if isinstance(by_date, dict):
                for day, value in by_date.items():
                    datasets[metric].append({
                        "calendarDate": str(day)[:10],
                        "source_payload": value,
                        "record_scope": "daily_metric",
                    })
            raw = indexed.get("raw")
            if raw is not None and not by_date:
                raw_parts = raw if isinstance(raw, list) else [raw]
                for part_index, part in enumerate(raw_parts):
                    datasets[metric].append({
                        "calendarDate": None,
                        "source_payload": part,
                        "record_scope": f"raw_range_{part_index}",
                    })

    by_date = health.get("by_date", {})
    if isinstance(by_date, dict):
        for day, values in by_date.items():
            if not isinstance(values, dict):
                continue
            for metric, value in values.items():
                if value is not None:
                    datasets[metric].append({
                        "calendarDate": str(day)[:10],
                        "source_payload": value,
                        "record_scope": "daily_snapshot",
                    })


def _list_json_files(dbx: Any, folder: str) -> list[dict[str, Any]]:
    result = dbx.files_list_folder(folder, recursive=False, limit=500)
    entries = list(result.entries)
    while result.has_more:
        result = dbx.files_list_folder_continue(result.cursor)
        entries.extend(result.entries)
    return [
        entry for entry in entries
        if getattr(entry, "name", "").endswith(".json")
        and getattr(entry, "name", "").startswith("garmin")
    ]


def _read_json(dbx: Any, path: str) -> dict[str, Any]:
    _, response = dbx.files_download(path)
    try:
        payload = json.loads(response.content)
    finally:
        response.close()
    if not isinstance(payload, dict):
        raise RuntimeError(f"{path}: expected a JSON object")
    return payload


def collect_archive(dbx: Any) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    files = []
    for folder in (HISTORY_DIR, CURRENT_DIR):
        for entry in _list_json_files(dbx, folder):
            name = entry.name
            if name.startswith("garmin_history_") or name.startswith("garmin_"):
                files.append((folder, entry))
    files.sort(key=lambda item: (item[0] == CURRENT_DIR, item[1].name))

    if not files:
        raise RuntimeError("No monthly Garmin JSON archives found in Dropbox")

    datasets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    activities_by_id: dict[str, dict[str, Any]] = {}
    devices_by_id: dict[str, dict[str, Any]] = {}
    gear_by_id: dict[str, dict[str, Any]] = {}
    file_activity_counts: dict[str, int] = {}

    for folder, entry in files:
        path = f"{folder}/{entry.name}"
        payload = _read_json(dbx, path)
        status = payload.get("metadata", {}).get("export_status")
        if status != "complete":
            raise RuntimeError(f"{path}: export_status={status!r}; refusing incomplete backfill")

        activities = payload.get("activities", [])
        if not isinstance(activities, list):
            raise RuntimeError(f"{path}: activities is not an array")
        ids = [_activity_id(row) for row in activities if isinstance(row, dict)]
        if len(ids) != len(activities) or None in ids or len(ids) != len(set(ids)):
            raise RuntimeError(f"{path}: missing or duplicate activity IDs")
        file_activity_counts[entry.name] = len(ids)
        for row in activities:
            activities_by_id[_activity_id(row)] = row
            # Older monthly archives may omit top-level registries while each
            # activity still carries its resolved device and gear assignment.
            device = row.get("device")
            if isinstance(device, dict):
                device_id = device.get("device_id", device.get("deviceId"))
                if device_id is not None:
                    devices_by_id[str(device_id)] = device
            else:
                summary = row.get("summary") if isinstance(row.get("summary"), dict) else {}
                detail = row.get("detail") if isinstance(row.get("detail"), dict) else {}
                device_id = summary.get("deviceId", detail.get("deviceId"))
                if device_id is not None:
                    devices_by_id[str(device_id)] = {
                        "device_id": str(device_id),
                        "device_model": None,
                        "source": "activity_record",
                    }
            gear = row.get("gear")
            if isinstance(gear, dict):
                gear_id = gear.get("gear_id", gear.get("gearPk", gear.get("gearId")))
                gear_meta = gear.get("gear")
                if gear_id is not None:
                    gear_by_id[str(gear_id)] = {
                        "gear_id": str(gear_id),
                        **(gear_meta if isinstance(gear_meta, dict) else {}),
                    }

        for device in payload.get("devices", []) or []:
            if not isinstance(device, dict):
                continue
            device_id = device.get("device_id", device.get("deviceId"))
            if device_id is not None:
                devices_by_id[str(device_id)] = device

        for gear in payload.get("gear_registry", []) or []:
            if not isinstance(gear, dict):
                continue
            gear_id = gear.get("gear_id", gear.get("gearPk", gear.get("gearId")))
            if gear_id is not None:
                gear_by_id[str(gear_id)] = gear

        health = payload.get("health")
        if not isinstance(health, dict):
            health = payload.get("daily")
        if isinstance(health, dict):
            _append_health(datasets, health)

        LOG.info("Preflight %s: %d activities", entry.name, len(ids))

    datasets["activities"] = list(activities_by_id.values())
    datasets["devices"] = list(devices_by_id.values())
    datasets["gear_registry"] = list(gear_by_id.values())

    # Drop exact duplicate health samples only; distinct samples on the same day survive.
    for source, rows in list(datasets.items()):
        if source in ("activities", "devices", "gear_registry"):
            continue
        unique: dict[str, dict[str, Any]] = {}
        for row in rows:
            # The same daily sample can appear in both range_data and by_date.
            # Collapse only byte-equivalent date/value pairs, not distinct samples.
            canonical = json.dumps(
                {"calendarDate": row.get("calendarDate"),
                 "source_payload": row.get("source_payload")},
                sort_keys=True, ensure_ascii=False, separators=(",", ":"))
            unique[canonical] = row
        datasets[source] = list(unique.values())

    stats = {
        "files": len(files),
        "activity_files": sum(file_activity_counts.values()),
        "unique_activities": len(activities_by_id),
        "devices": len(devices_by_id),
        "gear": len(gear_by_id),
        "metric_sources": len([k for k in datasets if k not in ("activities", "devices", "gear_registry")]),
    }
    return datasets, stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_id = str(uuid.uuid4())
    dbx = dbx_client()
    dbx.users_get_current_account()
    LOG.info("Reading complete Garmin archive from Dropbox")
    datasets, stats = collect_archive(dbx)
    LOG.info("Backfill preflight passed: %s; rows by source=%s", stats,
             {name: len(rows) for name, rows in datasets.items()})

    con = connect()
    try:
        ensure_schema(con)
        for source, rows in datasets.items():
            loaded = load_records(con, source, rows, run_id)
            LOG.info("Loaded %s: %d records", source, loaded)

        model_path = __import__("pathlib").Path(__file__).resolve().parents[1] / "sql" / "garmin_motherduck_model.sql"
        con.execute(model_path.read_text(encoding="utf-8"))

        actual_activities = con.execute("SELECT count(*) FROM core.activities").fetchone()[0]
        expected_activities = stats["unique_activities"]
        if actual_activities < expected_activities:
            raise RuntimeError(
                f"CORE activity validation failed: expected at least {expected_activities}, "
                f"found {actual_activities}"
            )
        duplicates = con.execute("""
            SELECT count(*) FROM (
                SELECT activity_id FROM core.activities
                GROUP BY activity_id HAVING count(*) > 1
            )
        """).fetchone()[0]
        if duplicates:
            raise RuntimeError(f"CORE activity validation found {duplicates} duplicate IDs")

        source_counts = con.execute("""
            SELECT metric_source, count(*) FROM core.daily_metrics
            GROUP BY metric_source ORDER BY metric_source
        """).fetchall()
        LOG.info(
            "BACKFILL VALIDATED run=%s files=%d unique_activities=%d core_activities=%d "
            "metric_sources=%d devices=%d gear=%d",
            run_id, stats["files"], expected_activities, actual_activities,
            len(source_counts),
            con.execute("SELECT count(*) FROM core.devices").fetchone()[0],
            con.execute("SELECT count(*) FROM core.gear").fetchone()[0],
        )
        for source, count in source_counts:
            LOG.info("CORE metric %s: %d", source, count)
    finally:
        con.close()


if __name__ == "__main__":
    main()
