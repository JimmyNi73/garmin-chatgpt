"""Full-history Garmin -> MotherDuck ingestion.

Uses the same Garmin Personal API client and detail validation as garmin_export.py.
No Dropbox reads/writes, month-partitioned database storage, or destructive
database operations. Gear and weight metadata are fetched directly from Garmin.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from garmin_export import (
    GarminCLI, activity_records, device_registry,
    fetch_activities, fetch_month_health, fetch_gear_data, enrich_gear_records,
)
from motherduck_loader import connect, ensure_schema, load_records

LOG = logging.getLogger("motherduck_export")
TZ = ZoneInfo("Europe/Stockholm")
def month_bounds(month: str) -> tuple[date, date]:
    first = date.fromisoformat(month + "-01")
    if first.month == 12:
        next_month = date(first.year + 1, 1, 1)
    else:
        next_month = date(first.year, first.month + 1, 1)
    return first, next_month - timedelta(days=1)


def date_of(row: dict) -> str | None:
    for key in ("calendarDate", "date", "measurementDate", "startTimeLocal",
                "startTimeGMT", "startDate", "summaryDate"):
        value = row.get(key)
        if value:
            return str(value)[:10]
    metadata = row.get("metaData")
    if isinstance(metadata, dict) and metadata.get("calendarDate"):
        return str(metadata["calendarDate"])[:10]
    return None


def weight_measurement_rows(value: object) -> list[dict]:
    """Find individual Garmin weight samples in nested/list API responses."""
    found: list[dict] = []
    seen: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        weight = node.get("weight")
        has_weight = (
            (isinstance(weight, dict) and weight.get("weight") is not None)
            or (isinstance(weight, (int, float)) and not isinstance(weight, bool)
                and date_of(node) is not None)
            or any(node.get(key) is not None for key in ("weightInGrams", "weightKg"))
        )
        if has_weight:
            day = date_of(node)
            if day:
                key = json.dumps(node, sort_keys=True, ensure_ascii=False, default=str)
                if key not in seen:
                    seen.add(key)
                    found.append({"calendarDate": day, "source_payload": node})
        for child in node.values():
            if isinstance(child, (dict, list)):
                walk(child)

    walk(value)
    return found


def health_records(source: str, health: dict, allowed_start: date,
                   allowed_end: date) -> list[dict]:
    """Flatten date-indexed and daily health data without collapsing same-day samples."""
    out: list[dict] = []
    range_data = health.get("range_data", {})
    for metric, indexed in range_data.items():
        if not isinstance(indexed, dict):
            continue
        by_date = indexed.get("by_date", {})
        raw = indexed.get("raw")
        if metric == "weight_measurements":
            # date_index is date-grained and would overwrite same-day weigh-ins.
            # Read the raw response first, then merge date-indexed fallbacks.
            candidates = weight_measurement_rows(raw) if raw is not None else []
            if isinstance(by_date, dict):
                candidates.extend(
                    {"calendarDate": str(day)[:10], "source_payload": payload}
                    for day, payload in by_date.items() if isinstance(payload, dict)
                )
            seen_samples: set[str] = set()
            for sample in candidates:
                try:
                    d = date.fromisoformat(sample["calendarDate"][:10])
                except (KeyError, ValueError):
                    continue
                if not (allowed_start <= d <= allowed_end):
                    continue
                signature = json.dumps(sample, sort_keys=True, ensure_ascii=False, default=str)
                if signature not in seen_samples:
                    seen_samples.add(signature)
                    out.append({"metric_source": metric, **sample})
            continue
        if isinstance(by_date, dict):
            for day, payload in by_date.items():
                try:
                    d = date.fromisoformat(str(day)[:10])
                except ValueError:
                    continue
                if allowed_start <= d <= allowed_end and isinstance(payload, dict):
                    out.append({"metric_source": metric, "calendarDate": d.isoformat(),
                                "source_payload": payload})
        if raw is not None and not by_date:
            # HRV summaries are returned as a rolling range, even when the caller
            # requested a narrower month/test window. Clip nested dates before
            # retaining the raw envelope so no out-of-window days leak into RAW.
            if metric == "hrv_summaries" and isinstance(raw, dict):
                raw = dict(raw)
                for key in ("hrvSummaries", "hrv_summaries"):
                    items = raw.get(key)
                    if isinstance(items, list):
                        raw[key] = [
                            item for item in items
                            if isinstance(item, dict)
                            and date_of(item) is not None
                            and allowed_start <= date.fromisoformat(date_of(item)) <= allowed_end
                        ]
            # Retain unindexed source response as one complete RAW record.
            out.append({"metric_source": metric, "calendarDate": None,
                        "source_payload": raw})
    for day, payload in health.get("by_date", {}).items():
        try:
            d = date.fromisoformat(str(day)[:10])
        except ValueError:
            continue
        if not (allowed_start <= d <= allowed_end) or not isinstance(payload, dict):
            continue
        for metric, value in payload.items():
            if value is not None:
                out.append({"metric_source": metric, "calendarDate": d.isoformat(),
                            "source_payload": value})
    return out


def load_in_batches(con, source: str, rows: list[dict], run_id: str,
                    batch_size: int) -> int:
    """Commit bounded, independently retryable batches to MotherDuck."""
    total = len(rows)
    loaded = 0
    for offset in range(0, total, batch_size):
        batch = rows[offset:offset + batch_size]
        count = load_records(con, source, batch, run_id)
        loaded += count
        LOG.info("MotherDuck batch: source=%s rows=%d/%d batch=%d",
                 source, loaded, total, (offset // batch_size) + 1)
    return loaded



def ensure_progress_table(con) -> None:
    """Durable checkpoints: completed health months and activity detail IDs."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.garmin_ingestion_progress (
            phase VARCHAR NOT NULL,
            item_key VARCHAR NOT NULL,
            completed_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (phase, item_key)
        )
    """)


def completed_items(con, phase: str) -> set[str]:
    return {
        str(row[0]) for row in con.execute(
            "SELECT item_key FROM raw.garmin_ingestion_progress WHERE phase = ?",
            [phase],
        ).fetchall()
    }


def mark_completed(con, phase: str, item_keys: list[str]) -> None:
    """Persist progress only after the corresponding data writes have committed."""
    if not item_keys:
        return
    con.executemany("""
        INSERT INTO raw.garmin_ingestion_progress (phase, item_key, completed_at)
        VALUES (?, ?, current_timestamp)
        ON CONFLICT (phase, item_key) DO UPDATE
        SET completed_at = excluded.completed_at
    """, [(phase, str(key)) for key in item_keys])


def initialize_full_rebuild(con, run_id: str) -> bool:
    """Truncate ingestion data once per GitHub run; preserve checkpoints on rerun."""
    ensure_progress_table(con)
    if run_id in completed_items(con, "full_rebuild_initialized"):
        LOG.warning("Resume existing full rebuild %s: data tables and checkpoints retained", run_id)
        return False
    for schema in ("raw", "core"):
        table_names = [
            row[0] for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = ? AND table_type = 'BASE TABLE'",
                [schema],
            ).fetchall()
        ]
        for table_name in table_names:
            if schema == "raw" and table_name == "garmin_ingestion_progress":
                continue
            if not table_name.replace("_", "").isalnum():
                raise RuntimeError(f"Unexpected table identifier: {table_name!r}")
            con.execute(f"TRUNCATE TABLE {schema}.{table_name}")
    # Write the marker only after truncation has fully completed. A crash before
    # this point is safe: no source writes have started and the next attempt truncates again.
    mark_completed(con, "full_rebuild_initialized", [run_id])
    LOG.warning("Started clean Garmin full rebuild %s; all RAW/CORE data tables truncated", run_id)
    return True


def load_checkpointed_batches(con, source: str, rows: list[dict], run_id: str,
                              batch_size: int, scope: str) -> int:
    """Load only batches without a matching durable content checkpoint.

    The key includes a hash of the canonical batch content. If Garmin changes
    data later, the changed batch is loaded instead of being skipped.
    """
    loaded = 0
    for offset in range(0, len(rows), batch_size):
        batch_index = offset // batch_size
        batch = rows[offset:offset + batch_size]
        canonical = json.dumps(batch, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        item_key = f"{scope}|{source}|{batch_index}|{digest}"
        if item_key in completed_items(con, f"health_batch:{run_id}"):
            LOG.info("Resume: source=%s scope=%s batch=%d already committed; skipping",
                     source, scope, batch_index + 1)
            continue
        count = load_records(con, source, batch, run_id)
        loaded += count
        # Checkpoint only after load_records has committed its transaction.
        mark_completed(con, f"health_batch:{run_id}", [item_key])
        LOG.info("MotherDuck checkpointed batch: source=%s scope=%s rows=%d batch=%d",
                 source, scope, count, batch_index + 1)
    return loaded


def main_full() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    if not os.getenv("GARMIN_PERSONAL_API_KEY") or not os.getenv("MOTHERDUCK_TOKEN"):
        raise RuntimeError("GARMIN_PERSONAL_API_KEY and MOTHERDUCK_TOKEN are required")
    history_mode = os.getenv("GARMIN_HISTORY_MODE", "full").strip().lower()
    if history_mode != "full":
        raise RuntimeError("GARMIN_HISTORY_MODE must be 'full'; partial ingestion is not supported")
    batch_size = int(os.getenv("MOTHERDUCK_BATCH_SIZE", "100"))
    if batch_size < 1:
        raise ValueError("MOTHERDUCK_BATCH_SIZE must be at least 1")
    LOG.info("Configured ingestion mode: %s; MotherDuck batch size: %d",
             history_mode, batch_size)

    today = datetime.now(TZ).date()
    run_id = os.getenv("GITHUB_RUN_ID") or str(uuid.uuid4())
    cli = GarminCLI()
    summaries_by_id: dict[str, dict] = {}

    # Fetch and validate the complete activity index before any writes.
    for year in range(2000, today.year + 1):
        year_start = date(year, 1, 1)
        year_end = min(date(year, 12, 31), today)
        LOG.info("Fetching Garmin activity index for %d (%s..%s)", year, year_start, year_end)
        for summary in fetch_activities(cli, year_start.isoformat(), year_end.isoformat()):
            aid = summary.get("activityId", summary.get("activity_id", summary.get("id")))
            if aid is None:
                raise RuntimeError(f"Garmin activity index {year}: missing activityId")
            if str(aid) in summaries_by_id:
                raise RuntimeError(f"Garmin activity index contains duplicate activityId: {aid}")
            summaries_by_id[str(aid)] = summary

    if not summaries_by_id:
        raise RuntimeError("Garmin returned no activity history; refusing an empty full load")
    activity_dates = []
    for summary in summaries_by_id.values():
        stamp = summary.get("startTimeLocal") or summary.get("startTimeGMT")
        if stamp:
            try:
                activity_dates.append(date.fromisoformat(str(stamp)[:10]))
            except ValueError:
                pass
    if not activity_dates:
        raise RuntimeError("Garmin activity index has no usable dates")

    # Gear is optional metadata; fetch it before activity batches so every
    # activity batch is enriched consistently.
    try:
        gear_registry, activity_to_gear = fetch_gear_data(cli)
    except Exception as exc:
        LOG.warning("Garmin gear registry unavailable (%s); continuing with direct activity gear IDs only", exc)
        gear_registry, activity_to_gear = {}, {}

    con = connect()
    total_loaded = 0
    activity_count = 0
    mapped_activities = 0
    weight_rows = 0
    try:
        ensure_schema(con)
        ensure_progress_table(con)

        # A new GitHub run starts with a clean rebuild. Re-running the same
        # workflow run preserves committed data and checkpoints, avoiding loss.
        initialize_full_rebuild(con, run_id)

        # Health is fetched and committed one month at a time. A failed later
        # month does not discard earlier committed work; stable keys make reruns
        # idempotent. Every month is validated before its records are written.
        health_month_phase = f"health_month:{run_id}"
        completed_health_months = completed_items(con, health_month_phase)
        first_month = min(activity_dates).replace(day=1)
        cursor = first_month
        while cursor <= today:
            month_start = cursor
            next_month = (date(cursor.year + 1, 1, 1) if cursor.month == 12
                          else date(cursor.year, cursor.month + 1, 1))
            month_end = min(next_month - timedelta(days=1), today)
            month = cursor.strftime("%Y-%m")
            if month in completed_health_months:
                LOG.info("Resume: health month %s already committed; skipping", month)
                cursor = next_month
                continue
            LOG.info("Fetching full-month Garmin health history %s (%s..%s)",
                     month, month_start, month_end)
            health = fetch_month_health(cli, month, today=today, full_month=True)
            if health.get("metadata", {}).get("export_status") != "complete":
                raise RuntimeError(
                    f"Health source errors for {month}: {health.get('source_errors', [])}")
            by_source: dict[str, list[dict]] = {}
            for row in health_records("health", health, month_start, month_end):
                source = row.pop("metric_source")
                by_source.setdefault(source, []).append(row)
            for source, rows in by_source.items():
                # Remove exact duplicates within this month without collapsing
                # separate same-day samples (notably weigh-ins).
                unique = {json.dumps(row, sort_keys=True, ensure_ascii=False, default=str): row
                          for row in rows}
                batch_rows = list(unique.values())
                total_loaded += load_checkpointed_batches(
                    con, source, batch_rows, run_id, batch_size, month)
                if source == "weight_measurements":
                    weight_rows += len(batch_rows)
            mark_completed(con, health_month_phase, [month])
            completed_health_months.add(month)
            LOG.info("Health month %s committed and checkpointed: %d source groups",
                     month, len(by_source))
            cursor = next_month

        # Fetch activity details in bounded groups. Each group is checked against
        # its matching summary IDs before it is committed.
        summaries = list(summaries_by_id.values())
        activity_phase = f"activity_detail:{run_id}"
        completed_activity_ids = completed_items(con, activity_phase)
        pending_summaries = [
            summary for summary in summaries
            if str(summary.get("activityId", summary.get("activity_id", summary.get("id"))))
            not in completed_activity_ids
        ]
        LOG.info("Resume: %d/%d activity details already committed; %d remain",
                 len(completed_activity_ids.intersection(summaries_by_id)),
                 len(summaries), len(pending_summaries))
        for offset in range(0, len(pending_summaries), batch_size):
            summary_batch = pending_summaries[offset:offset + batch_size]
            activities = activity_records(cli, summary_batch)
            expected_ids = {
                str(row.get("activityId", row.get("activity_id", row.get("id"))))
                for row in summary_batch
            }
            detail_ids = [
                str(row.get("summary", {}).get("activityId"))
                for row in activities
            ]
            if (len(detail_ids) != len(expected_ids)
                    or len(detail_ids) != len(set(detail_ids))
                    or set(detail_ids) != expected_ids):
                raise RuntimeError(
                    f"Activity detail batch preflight failed at offset {offset}; "
                    "batch not written")
            enrich_gear_records(activities, gear_registry, activity_to_gear)
            unknown_gear_ids = set(activity_to_gear.values()) - set(gear_registry)
            if unknown_gear_ids and offset == 0:
                LOG.warning("Gear assignments reference IDs absent from registry: %s",
                            sorted(unknown_gear_ids))
            total_loaded += load_in_batches(
                con, "activities", activities, run_id, batch_size)
            device_rows = device_registry(activities)
            if device_rows:
                total_loaded += load_in_batches(
                    con, "devices", device_rows, run_id, batch_size)
            batch_activity_ids = [
                str(row.get("summary", {}).get("activityId")) for row in activities
            ]
            mark_completed(con, activity_phase, batch_activity_ids)
            completed_activity_ids.update(batch_activity_ids)
            activity_count += len(activities)
            mapped_activities += sum(
                1 for row in activities
                if isinstance(row.get("gear"), dict)
                and row["gear"].get("gear_id") is not None)
            LOG.info("Activity detail progress: %d/%d activities committed",
                     activity_count, len(summaries))

        gear_rows = [
            {"gear_id": gear_id, **gear}
            for gear_id, gear in sorted(gear_registry.items())
        ]
        if gear_rows:
            total_loaded += load_in_batches(
                con, "gear_registry", gear_rows, run_id, batch_size)

        # VO2 values are derived from the already validated activity index.
        vo2 = []
        for summary in summaries_by_id.values():
            value = summary.get("vO2MaxValue")
            stamp = summary.get("startTimeLocal") or summary.get("startTimeGMT")
            if value is not None and stamp:
                vo2.append({"calendarDate": str(stamp)[:10],
                            "activityId": str(summary.get("activityId")),
                            "vo2MaxValue": value, "source_payload": summary})
        if vo2:
            total_loaded += load_in_batches(
                con, "vo2_max", vo2, run_id, batch_size)
        else:
            LOG.warning("No VO2 values returned in documented activity summaries")

        # Validate the committed source layer before publishing CORE/MART views.
        expected_months = []
        cursor = first_month
        while cursor <= today:
            expected_months.append(cursor.strftime("%Y-%m"))
            cursor = (date(cursor.year + 1, 1, 1) if cursor.month == 12
                      else date(cursor.year, cursor.month + 1, 1))
        committed_months = completed_items(con, health_month_phase)
        missing_months = sorted(set(expected_months) - committed_months)
        if missing_months:
            raise RuntimeError(f"Health month checkpoint validation failed: {missing_months[:10]}")
        committed_activity_ids = completed_items(con, activity_phase)
        missing_activity_ids = set(summaries_by_id) - committed_activity_ids
        if missing_activity_ids:
            raise RuntimeError(
                f"Activity checkpoint validation failed: {len(missing_activity_ids)} IDs missing")

        raw_activity_ids = {
            str(row[0]) for row in con.execute(
                "SELECT record_key FROM raw.activities"
            ).fetchall()
        }
        if raw_activity_ids != set(summaries_by_id):
            raise RuntimeError(
                f"RAW activity ID parity failed: Garmin={len(summaries_by_id)}, "
                f"MotherDuck={len(raw_activity_ids)}, missing={len(set(summaries_by_id)-raw_activity_ids)}, "
                f"unexpected={len(raw_activity_ids-set(summaries_by_id))}")

        # Every source-specific RAW table must exactly mirror generic RAW keys/payloads.
        source_counts = con.execute("""
            SELECT source_name, count(*) FROM raw.garmin_payloads
            GROUP BY source_name ORDER BY source_name
        """).fetchall()
        for source_name, _ in source_counts:
            table_name = source_name.lower()
            if not table_name.replace("_", "").isalnum():
                raise RuntimeError(f"Unexpected RAW source name: {source_name!r}")
            mismatch = con.execute(f"""
                SELECT count(*) FROM raw.garmin_payloads g
                FULL OUTER JOIN raw.{table_name} s ON s.record_key = g.record_key
                WHERE (g.source_name = ? OR g.source_name IS NULL)
                  AND (g.record_key IS NULL OR s.record_key IS NULL
                       OR g.payload IS DISTINCT FROM s.payload)
            """, [source_name]).fetchone()[0]
            if mismatch:
                raise RuntimeError(
                    f"RAW parity failed for {source_name}: {mismatch} missing/different records")

        # Publish relational models only after source/checkpoint parity passes.
        model_path = Path(__file__).resolve().parents[1] / "sql" / "garmin_motherduck_model.sql"
        con.execute(model_path.read_text(encoding="utf-8"))
        core_activity_ids = {
            str(row[0]) for row in con.execute("SELECT activity_id FROM core.activities").fetchall()
        }
        if core_activity_ids != set(summaries_by_id):
            raise RuntimeError(
                f"CORE activity ID parity failed: Garmin={len(summaries_by_id)}, "
                f"CORE={len(core_activity_ids)}")
        mapped_count = con.execute(
            "SELECT count(*) FROM core.activities WHERE gear_id IS NOT NULL"
        ).fetchone()[0]
        weight_count = con.execute(
            "SELECT count(*) FROM raw.weight_measurements"
        ).fetchone()[0] if con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema='raw' AND table_name='weight_measurements'"
        ).fetchone()[0] else 0
        LOG.info("Garmin full-history ingestion succeeded: run=%s activities=%d gear_mapped=%d "
                 "weight_raw_records=%d requests=%d source_counts=%s",
                 run_id, len(summaries_by_id), mapped_count, weight_count,
                 cli.request_count, source_counts)
    finally:
        con.close()



def activity_day(summary: dict) -> date:
    """Parse Garmin activity timestamps returned as ISO strings or epoch milliseconds."""
    raw = summary.get("startTimeLocal") or summary.get("startTimeGMT")
    if raw is None:
        raise RuntimeError(f"Activity {summary.get('activityId')} has no date")
    try:
        if isinstance(raw, (int, float)) or str(raw).isdigit():
            stamp = float(raw)
            if stamp > 10**11:
                stamp /= 1000
            return datetime.fromtimestamp(stamp, TZ).date()
        return date.fromisoformat(str(raw)[:10])
    except (ValueError, TypeError, OverflowError) as exc:
        raise RuntimeError(f"Unparseable activity date for {summary.get('activityId')}: {raw}") from exc


def truncate_source_tables(con) -> None:
    """Clear every physical ingestion table; views are derived and must not be truncated."""
    tables = (
        "raw.garmin_payloads",
        "core.activities",
        "core.activity_workouts",
        "core.daily_metrics",
        "core.devices",
        "core.gear",
    )
    for table in tables:
        LOG.warning("TRUNCATING %s", table)
        con.execute(f"TRUNCATE TABLE {table}")


def main_test() -> None:
    """Destructive, bounded test load: one month in 2011, one in 2022, latest 14 days."""
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    if not os.getenv("GARMIN_PERSONAL_API_KEY") or not os.getenv("MOTHERDUCK_TOKEN"):
        raise RuntimeError("GARMIN_PERSONAL_API_KEY and MOTHERDUCK_TOKEN are required")
    batch_size = int(os.getenv("MOTHERDUCK_BATCH_SIZE", "100"))
    if batch_size < 1:
        raise ValueError("MOTHERDUCK_BATCH_SIZE must be at least 1")

    today = datetime.now(TZ).date()
    month_2011 = os.getenv("GARMIN_TEST_MONTH_2011", "2011-12")
    month_2022 = os.getenv("GARMIN_TEST_MONTH_2022", "2022-05")
    start_2011, end_2011 = month_bounds(month_2011)
    start_2022, end_2022 = month_bounds(month_2022)
    recent_start, recent_end = today - timedelta(days=13), today
    windows = [
        (f"month_{month_2011}", start_2011, end_2011),
        (f"month_{month_2022}", start_2022, end_2022),
        ("latest_14_days", recent_start, recent_end),
    ]
    LOG.info("TEST windows: %s", [(n, s.isoformat(), e.isoformat()) for n, s, e in windows])

    cli = GarminCLI()
    run_id = str(uuid.uuid4())

    # Retrieve and preflight every source window BEFORE truncating anything.
    summaries_by_id: dict[str, dict] = {}
    for label, start, end in windows:
        LOG.info("Fetching activity index for %s (%s..%s)", label, start, end)
        summaries = fetch_activities(cli, start.isoformat(), end.isoformat())
        matching = [s for s in summaries if start <= activity_day(s) <= end]
        if label != "latest_14_days" and not matching:
            raise RuntimeError(f"No Garmin activities found in required test window {label}")
        for summary in matching:
            aid = summary.get("activityId", summary.get("activity_id", summary.get("id")))
            if aid is None:
                raise RuntimeError(f"Garmin activity index {label}: missing activityId")
            summaries_by_id[str(aid)] = summary
        LOG.info("Activity summaries for %s: %d", label, len(matching))

    if not summaries_by_id:
        raise RuntimeError("Garmin returned no activities in any test window")

    # Gear is optional metadata, matching the production ingestion behavior.
    try:
        gear_registry, activity_to_gear = fetch_gear_data(cli)
    except Exception as exc:
        LOG.warning("Gear registry unavailable (%s); continuing without gear metadata", exc)
        gear_registry, activity_to_gear = {}, {}

    # Fetch all three health windows before the destructive truncate.
    health_groups: dict[str, list[dict]] = {}
    weight_rows = 0
    health_months = sorted({month_2011, month_2022, today.strftime("%Y-%m")})
    for month in health_months:
        health = fetch_month_health(cli, month, today=today)
        if health.get("metadata", {}).get("export_status") != "complete":
            raise RuntimeError(
                f"Health source errors for {month}: {health.get('source_errors', [])}")
        month_start, month_end = month_bounds(month)
        if month == today.strftime("%Y-%m"):
            allowed_start, allowed_end = recent_start, recent_end
        elif month == month_2011:
            allowed_start, allowed_end = start_2011, end_2011
        else:
            allowed_start, allowed_end = start_2022, end_2022
        for row in health_records("health", health, allowed_start, allowed_end):
            source = row.pop("metric_source")
            health_groups.setdefault(source, []).append(row)

    # Activity detail calls are bounded and validated against the exact summary IDs.
    summaries = list(summaries_by_id.values())
    activity_batches: list[list[dict]] = []
    for offset in range(0, len(summaries), batch_size):
        summary_batch = summaries[offset:offset + batch_size]
        activities = activity_records(cli, summary_batch)
        expected_ids = {
            str(row.get("activityId", row.get("activity_id", row.get("id"))))
            for row in summary_batch
        }
        detail_ids = [
            str(row.get("summary", {}).get("activityId"))
            for row in activities
        ]
        if (len(detail_ids) != len(expected_ids)
                or len(detail_ids) != len(set(detail_ids))
                or set(detail_ids) != expected_ids):
            raise RuntimeError(
                f"Activity detail batch preflight failed at offset {offset}; nothing truncated")
        enrich_gear_records(activities, gear_registry, activity_to_gear)
        activity_batches.append(activities)

    # All Garmin retrieval and activity preflight succeeded. Only now clear tables.
    con = connect()
    total_loaded = 0
    activity_count = 0
    mapped_activities = 0
    try:
        ensure_schema(con)
        truncate_source_tables(con)

        for source, rows in health_groups.items():
            unique = {
                json.dumps(row, sort_keys=True, ensure_ascii=False, default=str): row
                for row in rows
            }
            batch_rows = list(unique.values())
            total_loaded += load_in_batches(con, source, batch_rows, run_id, batch_size)
            if source == "weight_measurements":
                weight_rows += len(batch_rows)

        for activities in activity_batches:
            total_loaded += load_in_batches(
                con, "activities", activities, run_id, batch_size)
            device_rows = device_registry(activities)
            if device_rows:
                total_loaded += load_in_batches(
                    con, "devices", device_rows, run_id, batch_size)
            activity_count += len(activities)
            mapped_activities += sum(
                1 for row in activities
                if isinstance(row.get("gear"), dict)
                and row["gear"].get("gear_id") is not None)

        gear_rows = [
            {"gear_id": gear_id, **gear}
            for gear_id, gear in sorted(gear_registry.items())
        ]
        if gear_rows:
            total_loaded += load_in_batches(
                con, "gear_registry", gear_rows, run_id, batch_size)

        vo2 = []
        for summary in summaries:
            value = summary.get("vO2MaxValue")
            if value is not None:
                vo2.append({
                    "calendarDate": activity_day(summary).isoformat(),
                    "activityId": str(summary.get("activityId")),
                    "vo2MaxValue": value,
                    "source_payload": summary,
                })
        if vo2:
            total_loaded += load_in_batches(con, "vo2_max", vo2, run_id, batch_size)

        model_path = Path(__file__).resolve().parents[1] / "sql" / "garmin_motherduck_model.sql"
        con.execute(model_path.read_text(encoding="utf-8"))
        counts = con.execute("""
            SELECT source_name, count(*) FROM raw.garmin_payloads
            GROUP BY source_name ORDER BY source_name
        """).fetchall()
        LOG.info(
            "TEST ingestion succeeded: run=%s activities=%d gear_mapped=%d "
            "weight_rows=%d loaded=%d requests=%d source_counts=%s",
            run_id, activity_count, mapped_activities, weight_rows,
            total_loaded, cli.request_count, counts)
    finally:
        con.close()


def main() -> None:
    mode = os.getenv("GARMIN_HISTORY_MODE", "full").strip().lower()
    if mode == "test":
        main_test()
    elif mode == "full":
        main_full()
    else:
        raise RuntimeError("GARMIN_HISTORY_MODE must be 'full' or 'test'")


if __name__ == "__main__":
    main()

