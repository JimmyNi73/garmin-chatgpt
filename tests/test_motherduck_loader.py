import unittest

from src.motherduck_loader import event_date, stable_record_key


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


if __name__ == "__main__":
    unittest.main()
