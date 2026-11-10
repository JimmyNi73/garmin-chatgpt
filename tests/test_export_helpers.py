import json
import sys
import unittest
from pathlib import Path
import tempfile
from types import SimpleNamespace

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

    def test_month_health_clamps_range_to_today(self):
        class RecordingCLI:
            def __init__(self):
                self.calls = []
            def run(self, label, *args):
                self.calls.append((label, args))
                return []

        cli = RecordingCLI()
        result = ge.fetch_month_health(cli, "2026-11", today=ge.date(2026, 11, 7))

        for source in ("stress", "steps"):
            calls = [(label, args) for label, args in cli.calls
                     if label.startswith(source + " ")]
            self.assertEqual(len(calls), 2)
            ranges = []
            for label, args in calls:
                ranges.append((args[args.index("--start-date") + 1],
                               args[args.index("--end-date") + 1]))
            self.assertEqual(ranges, [("2026-10-09", "2026-10-31"),
                                      ("2026-11-01", "2026-11-07")])
        self.assertEqual(len(result["by_date"]), 30)

    def test_stress_and_steps_merge_month_segments(self):
        class SegmentCLI:
            def __init__(self):
                self.calls = []
            def run(self, label, *args):
                self.calls.append((label, args))
                if "--start-date" not in args:
                    return None
                start = args[args.index("--start-date") + 1]
                end = args[args.index("--end-date") + 1]
                return [{"calendarDate": start, "value": 11},
                        {"calendarDate": end, "value": 22}]

        cli = SegmentCLI()
        result = ge.fetch_month_health(cli, "2026-11", today=ge.date(2026, 11, 7))
        for field in ("stress", "steps"):
            indexed = result["range_data"][field]["by_date"]
            self.assertEqual(indexed["2026-10-09"]["value"], 11)
            self.assertEqual(indexed["2026-10-31"]["value"], 22)
            self.assertEqual(indexed["2026-11-01"]["value"], 11)
            self.assertEqual(indexed["2026-11-07"]["value"], 22)


    def test_gear_only_backfill_preserves_non_gear_data(self):
        class FakeResponse:
            def __init__(self, content):
                self.content = content
            def close(self):
                pass

        class FakeDropbox:
            def __init__(self):
                self.files = {
                    "/History/garmin_history_2011-01.json": {
                        "metadata": {"export_status": "complete", "activity_count": 1},
                        "activities": [{"summary": {"activityId": 123}, "gear": None,
                                        "detail": {"keep": "unchanged"}}],
                        "health": {"by_date": {"2011-01-01": {"steps": 10}}},
                        "profile": {"keep": True},
                    },
                    "/Current/garmin_2026-11.json": {
                        "metadata": {"export_status": "complete", "activity_count": 1},
                        "activities": [{"summary": {"activityId": 456}, "gear": None}],
                        "health": {"untouched": True},
                    },
                }
                self.uploaded = []

            def files_list_folder(self, folder):
                entries = [
                    SimpleNamespace(path_display=path, name=path.rsplit("/", 1)[-1])
                    for path in self.files if path.startswith(folder + "/")
                ]
                return SimpleNamespace(entries=entries, has_more=False, cursor=None)

            def files_download(self, path):
                return None, FakeResponse(json.dumps(self.files[path]).encode("utf-8"))

            def files_upload(self, data, path, mode=None, mute=True):
                self.files[path] = json.loads(data.decode("utf-8"))
                self.uploaded.append(path)

        dbx = FakeDropbox()
        original_output = ge.OUTPUT_DIR
        original_write_mode = ge.WriteMode
        # Keep this legacy Dropbox-helper unit test independent of the SDK.
        ge.WriteMode = SimpleNamespace(overwrite="overwrite")
        with tempfile.TemporaryDirectory() as tmpdir:
            ge.OUTPUT_DIR = Path(tmpdir)
            try:
                updated, activities, links = ge.backfill_gear_in_dropbox(
                    dbx,
                    {7: {"customMakeModel": "Brooks"}},
                    {"123": 7},
                )
            finally:
                ge.OUTPUT_DIR = original_output
                ge.WriteMode = original_write_mode

        self.assertEqual((updated, activities, links), (2, 2, 1))
        historical = dbx.files["/History/garmin_history_2011-01.json"]
        current = dbx.files["/Current/garmin_2026-11.json"]
        self.assertEqual(historical["activities"][0]["gear"]["gear_id"], 7)
        self.assertIsNone(current["activities"][0]["gear"])
        self.assertEqual(historical["health"], {"by_date": {"2011-01-01": {"steps": 10}}})
        self.assertEqual(historical["profile"], {"keep": True})
        self.assertEqual(historical["activities"][0]["detail"], {"keep": "unchanged"})
        self.assertEqual(historical["metadata"]["export_status"], "complete")
        self.assertEqual(historical["metadata"]["gear_link_count"], 1)
        self.assertEqual(current["metadata"]["gear_link_count"], 0)

    def test_gear_backfill_rejects_malformed_archive_before_writing(self):
        class FakeDropbox:
            def __init__(self):
                self.uploaded = []
            def files_list_folder(self, folder):
                entries = ([SimpleNamespace(path_display=folder + "/garmin_history_2011-01.json",
                                            name="garmin_history_2011-01.json")]
                           if folder == ge.HISTORY_DIR else [])
                return SimpleNamespace(entries=entries, has_more=False, cursor=None)
            def files_download(self, path):
                return None, SimpleNamespace(content=b'{"activities": "not-an-array", "metadata": {}}',
                                              close=lambda: None)
            def files_upload(self, *args, **kwargs):
                self.uploaded.append(args[1])

        dbx = FakeDropbox()
        with self.assertRaises(RuntimeError):
            ge.backfill_gear_in_dropbox(dbx, {7: {"name": "Brooks"}}, {})
        self.assertEqual(dbx.uploaded, [])


if __name__ == "__main__":
    unittest.main()
