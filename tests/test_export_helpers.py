import sys
import unittest

sys.path.insert(0, "src")
import garmin_export as ge


class FakeCLI:
    def run(self, label, *args):
        raise RuntimeError("HTTP 404 Not Found")


class ExportHelperTests(unittest.TestCase):
    def test_fenix6_device_override(self):
        row = ge.enrich_device_ids({
            "summary": {"activityId": 123, "deviceId": 3359313181}
        })
        info = ge.device_info_for_activity(row)
        self.assertEqual(info["device_model"], "Garmin fēnix 6 Sapphire")
        self.assertEqual(info["device_id"], "3359313181")

    def test_missing_daily_value_is_not_an_error(self):
        errors = []
        ok, value = ge.optional(FakeCLI(), errors, "sleep 2021-01-01",
                                "sleep", "--date", "2021-01-01")
        self.assertTrue(ok)
        self.assertIsNone(value)
        self.assertEqual(errors, [])

    def test_gear_association_uses_activity_id(self):
        rows = [{"summary": {"activityId": 456}}]
        registry = {42: {"customMakeModel": "Saucony Triumph"}}
        ge.enrich_gear_records(rows, registry, {"456": 42})
        self.assertEqual(rows[0]["gear"]["gear_id"], 42)
        self.assertEqual(rows[0]["gear"]["gear"]["customMakeModel"], "Saucony Triumph")

    def test_unknown_device_is_null_not_guessed(self):
        row = ge.enrich_device_ids({"detail": {"deviceId": 999999}})
        self.assertIsNone(row["detail"]["device_model"])


if __name__ == "__main__":
    unittest.main()
