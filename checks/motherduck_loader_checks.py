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


    def test_activity_load_handles_existing_schema_column_order(self):
        con = duckdb.connect(":memory:")
        self.addCleanup(con.close)
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE SCHEMA core")
        con.execute("CREATE SCHEMA mart")
        con.execute("""
            CREATE TABLE core.activities (
                activity_id VARCHAR PRIMARY KEY, activity_date DATE, activity_type VARCHAR,
                activity_name VARCHAR, distance_m DOUBLE, duration_s DOUBLE,
                elapsed_duration_s DOUBLE, average_speed_m_s DOUBLE,
                average_hr DOUBLE, max_hr DOUBLE, calories DOUBLE,
                average_run_cadence DOUBLE, max_run_cadence DOUBLE, vo2_max DOUBLE,
                device_id VARCHAR, gear_id VARCHAR, source_payload JSON NOT NULL,
                source_run_id VARCHAR NOT NULL, loaded_at TIMESTAMPTZ NOT NULL
            )
        """)
        ensure_schema(con)

        activity = {
            "summary": {
                "activityId": "legacy-order-1",
                "startTimeLocal": "2024-01-03 07:00:00",
                "activityType": {"typeKey": "running"},
                "distance": 5000,
                "duration": 1500,
                "averageHR": 142,
                "averageRunCadence": 170,
                "maxRunCadence": 180,
                "vO2MaxValue": 49.5,
                "deviceId": "device-1",
            },
            "detail": {
                "summaryDTO": {
                    "avgStrideLength": 1120,
                    "aerobicTrainingEffect": 3.4,
                    "activityTrainingLoad": 68,
                }
            },
            "gear": {"gear_id": "gear-1"},
        }
        self.assertEqual(load_records(con, "activities", [activity], "schema-run"), 1)
        actual = con.execute("""
            SELECT vo2_max, device_id, gear_id, stride_length_mm,
                   aerobic_training_effect, activity_training_load
            FROM core.activities WHERE activity_id = 'legacy-order-1'
        """).fetchone()
        self.assertEqual(actual, (49.5, "device-1", "gear-1", 1120.0, 3.4, 68.0))

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



    def test_daily_metric_key_uses_source_date(self):
        first = {"calendarDate": "2026-11-10", "source_payload": {"steps": 5000}}
        revised = {"calendarDate": "2026-11-10", "source_payload": {"steps": 6200}}
        self.assertEqual(
            stable_record_key("steps", first, 0),
            stable_record_key("steps", revised, 1),
        )

    def test_weight_samples_on_same_date_remain_distinct(self):
        first = {"calendarDate": "2026-11-10", "measurementId": "w-1",
                 "source_payload": {"weightKg": 84.2}}
        second = {"calendarDate": "2026-11-10", "measurementId": "w-2",
                  "source_payload": {"weightKg": 84.0}}
        self.assertNotEqual(
            stable_record_key("weight_measurements", first, 0),
            stable_record_key("weight_measurements", second, 1),
        )

    def test_daily_metric_rerun_updates_existing_day(self):
        con = duckdb.connect(":memory:")
        self.addCleanup(con.close)
        ensure_schema(con)
        first = {"calendarDate": "2026-11-10", "source_payload": {"steps": 5000}}
        revised = {"calendarDate": "2026-11-10", "source_payload": {"steps": 6200}}
        load_records(con, "steps", [first], "run-1")
        load_records(con, "steps", [revised], "run-2")
        self.assertEqual(
            con.execute("SELECT count(*) FROM core.daily_metrics WHERE metric_source='steps'").fetchone()[0],
            1,
        )
        self.assertEqual(
            con.execute("SELECT json_extract_string(metric_value, '$.steps') FROM core.daily_metrics WHERE metric_source='steps'").fetchone()[0],
            "6200",
        )


if __name__ == "__main__":
    unittest.main()
