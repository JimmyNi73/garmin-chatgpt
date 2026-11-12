import sys
import unittest
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from motherduck_export import (  # noqa: E402
    ensure_progress_table,
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
            self.con, "steps", rows, "run-2", 1, "2024-01"
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
                "SELECT count(*) FROM raw.garmin_ingestion_progress WHERE phase='health_batch'"
            ).fetchone()[0],
            3,
        )

    def test_repeating_completed_batches_does_not_create_duplicates(self):
        rows = [
            {"calendarDate": "2024-02-01", "source_payload": {"steps": 1234}},
            {"calendarDate": "2024-02-02", "source_payload": {"steps": 2345}},
        ]
        for run_id in ("run-1", "run-2"):
            load_checkpointed_batches(
                self.con, "steps", rows, run_id, 1, "2024-02"
            )
        self.assertEqual(
            self.con.execute(
                "SELECT count(*) FROM raw.garmin_payloads WHERE source_name='steps'"
            ).fetchone()[0],
            2,
        )


if __name__ == "__main__":
    unittest.main()
