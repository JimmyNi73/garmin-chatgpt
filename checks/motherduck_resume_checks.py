import sys
import unittest
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from motherduck_export import (  # noqa: E402
    ensure_progress_table,
    health_records,
    initialize_full_rebuild,
    load_checkpointed_batches,
)
from motherduck_loader import ensure_schema  # noqa: E402


class ResumeCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(":memory:")
        ensure_schema(self.con)
        ensure_progress_table(self.con)
        self.addCleanup(self.con.close)

    def test_resume_skips_committed_batch_and_loads_remaining_batches(self):
        rows = [
            {"calendarDate": "2024-01-01", "source_payload": {"steps": 100}},
            {"calendarDate": "2024-01-02", "source_payload": {"steps": 200}},
            {"calendarDate": "2024-01-03", "source_payload": {"steps": 300}},
        ]

        # Simulate interruption after the first batch has committed.
        first_loaded = load_checkpointed_batches(
            self.con, "steps", rows[:1], "run-1", 1, "2024-01"
        )
        self.assertEqual(first_loaded, 1)

        # A retry starts from the month, but skips the committed batch.
        resumed_loaded = load_checkpointed_batches(
            self.con, "steps", rows, "run-1", 1, "2024-01"
        )
        self.assertEqual(resumed_loaded, 2)
        self.assertEqual(
            self.con.execute(
                "SELECT count(*) FROM raw.garmin_payloads WHERE source_name='steps'"
            ).fetchone()[0],
            3,
        )
        self.assertEqual(
            self.con.execute(
                "SELECT count(*) FROM raw.garmin_ingestion_progress WHERE phase='health_batch:run-1'"
            ).fetchone()[0],
            3,
        )

    def test_repeating_completed_batches_does_not_create_duplicates(self):
        rows = [
            {"calendarDate": "2024-02-01", "source_payload": {"steps": 1234}},
            {"calendarDate": "2024-02-02", "source_payload": {"steps": 2345}},
        ]
        for _ in range(2):
            load_checkpointed_batches(
                self.con, "steps", rows, "run-1", 1, "2024-02"
            )
        self.assertEqual(
            self.con.execute(
                "SELECT count(*) FROM raw.garmin_payloads WHERE source_name='steps'"
            ).fetchone()[0],
            2,
        )



    def test_same_run_restart_keeps_data_and_checkpoints_after_reinitialization(self):
        rows = [
            {"calendarDate": "2024-03-01", "source_payload": {"steps": 300}},
            {"calendarDate": "2024-03-02", "source_payload": {"steps": 400}},
        ]
        self.assertTrue(initialize_full_rebuild(self.con, "run-1"))
        self.assertEqual(load_checkpointed_batches(
            self.con, "steps", rows[:1], "run-1", 1, "2024-03"), 1)
        self.assertFalse(initialize_full_rebuild(self.con, "run-1"))
        self.assertEqual(load_checkpointed_batches(
            self.con, "steps", rows, "run-1", 1, "2024-03"), 1)
        self.assertEqual(self.con.execute(
            "SELECT count(*) FROM raw.garmin_payloads WHERE source_name='steps'"
        ).fetchone()[0], 2)

    def test_new_run_starts_clean_but_preserves_old_run_checkpoints(self):
        row = [{"calendarDate": "2024-04-01", "source_payload": {"steps": 500}}]
        self.assertTrue(initialize_full_rebuild(self.con, "run-1"))
        load_checkpointed_batches(self.con, "steps", row, "run-1", 1, "2024-04")
        self.assertTrue(initialize_full_rebuild(self.con, "run-2"))
        self.assertEqual(self.con.execute(
            "SELECT count(*) FROM raw.garmin_payloads"
        ).fetchone()[0], 0)
        self.assertEqual(load_checkpointed_batches(
            self.con, "steps", row, "run-2", 1, "2024-04"), 1)
        self.assertEqual(self.con.execute(
            "SELECT count(*) FROM raw.garmin_payloads"
        ).fetchone()[0], 1)


    def test_health_raw_keeps_range_envelope_and_indexed_rows(self):
        from datetime import date
        health = {
            "range_data": {
                "steps": {
                    "raw": {"dailySummaries": [{"calendarDate": "2024-05-01", "steps": 7000}], "meta": "kept"},
                    "by_date": {"2024-05-01": {"calendarDate": "2024-05-01", "steps": 7000}},
                }
            },
            "by_date": {},
        }
        rows = health_records("health", health, date(2024, 5, 1), date(2024, 5, 31))
        self.assertEqual(len(rows), 2)
        envelope = next(row for row in rows if row.get("record_scope") == "range_response")
        self.assertEqual(envelope["source_payload"]["meta"], "kept")
        self.assertEqual(envelope["calendarDate"], None)
        self.assertEqual(rows[0]["source_payload"]["steps"], 7000)

if __name__ == "__main__":
    unittest.main()
