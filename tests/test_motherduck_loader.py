import unittest

import duckdb

from src.motherduck_loader import (
    ensure_schema, event_date, load_records, stable_record_key,
)


class LoaderTests(unittest.TestCase):
    def test_event_date_from_calendar_date(self):
        self.assertEqual(event_date({"calendarDate": "2026-11-10"}), "2026-11-10")

    def test_event_date_from_activity_timestamp(self):
        self.assertEqual(event_date({"startTimeLocal": "2026-11-10 07:30:00"}), "2026-11-10")

    def test_stable_key_uses_activity_id(self):
        self.assertEqual(stable_record_key("activities", {"activityId": 123}, 0), "123")

    def test_nested_summary_activity_id_wins_over_generic_id(self):
        self.assertEqual(
            stable_record_key("activities", {"id": "hash", "summary": {"activityId": 123}}, 0),
            "123",
        )

    def test_stable_key_is_deterministic_for_payload(self):
        row = {"foo": "bar", "value": 1}
        self.assertEqual(
            stable_record_key("daily", row, 0),
            stable_record_key("daily", row, 0),
        )

    def test_bulk_activity_and_metric_loads(self):
        con = duckdb.connect(":memory:")
        self.addCleanup(con.close)
        ensure_schema(con)

        activities = [
            {
                "summary": {
                    "activityId": "a-1", "startTimeLocal": "2024-01-01 07:00:00",
                    "activityType": {"typeKey": "running"}, "distance": 1000,
                    "duration": 300, "averageHR": 140,
                },
                "device": {"device_id": "d-1", "device_model": "Garmin epix"},
                "gear": {"gear_id": "g-1", "gear": {"name": "Running shoe"}},
                "workouts": [{"steps": []}],
            },
            {
                "summary": {
                    "activityId": "a-2", "startTimeLocal": "2024-01-02 07:00:00",
                    "activityType": {"typeKey": "running"}, "distance": 2000,
                    "duration": 600, "averageHR": 145,
                },
                "workouts": [],
            },
        ]
        self.assertEqual(load_records(con, "activities", activities, "test-run"), 2)
        metrics = [
            {"calendarDate": "2024-01-01", "source_payload": {"steps": 5000}},
            {"calendarDate": "2024-01-02", "source_payload": {"steps": 6000}},
        ]
        self.assertEqual(load_records(con, "steps", metrics, "test-run"), 2)

        self.assertEqual(con.execute("SELECT count(*) FROM core.activities").fetchone()[0], 2)
        self.assertEqual(con.execute("SELECT count(*) FROM core.activity_workouts").fetchone()[0], 1)
        self.assertEqual(con.execute("SELECT count(*) FROM core.daily_metrics").fetchone()[0], 2)
        self.assertEqual(
            con.execute("SELECT device_id, gear_id FROM core.activities WHERE activity_id='a-1'").fetchone(),
            ("d-1", "g-1"),
        )


if __name__ == "__main__":
    unittest.main()
