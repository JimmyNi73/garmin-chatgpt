import json
import sys
import unittest
from pathlib import Path
import tempfile
from types import SimpleNamespace

sys.path.insert(0, "src")
import garmin_export as ge
import motherduck_export as me


class FakeCLI:
    def run(self, label, *args):
        raise RuntimeError("HTTP 404 Not Found")


class ExportHelperTests(unittest.TestCase):
    def test_retry_after_reads_cloudflare_retry_after(self):
        self.assertEqual(
            ge.retry_after('{"status":504,"retry_after":120}'),
            120,
        )

    def test_retry_after_keeps_rate_limit_fallback(self):
        self.assertEqual(ge.retry_after("HTTP 429 Too Many Requests"), 30)

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

    def test_gear_direct_activity_id_survives_unavailable_registry(self):
        rows = [{"summary": {"activityId": 789}, "detail": {"gearPk": 17}}]
        ge.enrich_gear_records(rows, {}, {})
        self.assertEqual(rows[0]["gear"]["gear_id"], 17)
        self.assertIsNone(rows[0]["gear"]["gear"])

    def test_unknown_device_is_null_not_guessed(self):
        row = ge.enrich_device_ids({"detail": {"deviceId": 999999}})
        self.assertIsNone(row["detail"]["device_model"])

    def test_weight_samples_preserve_multiple_measurements_on_same_day(self):
        payload = [
            {"metaData": {"calendarDate": "2026-11-06T08:00:00.0", "sequence": 1},
             "weight": {"weight": 84300.0}},
            {"metaData": {"calendarDate": "2026-11-06T09:01:20.0", "sequence": 2},
             "weight": {"weight": 84400.0}},
        ]
        rows = me.weight_measurement_rows(payload)
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["calendarDate"] for row in rows}, {"2026-11-06"})

    def test_weight_samples_parse_scalar_weight_in_daily_weight_summaries(self):
        sample = {
            "calendarDate": "2022-05-31",
            "timestampGMT": 1653979050007,
            "weight": 87500.0,
            "sourceType": "MANUAL",
        }
        payload = {"dailyWeightSummaries": [{
            "allWeightMetrics": [sample],
            "latestWeight": sample,
        }]}
        rows = me.weight_measurement_rows(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["calendarDate"], "2022-05-31")
        self.assertEqual(rows[0]["source_payload"]["weight"], 87500.0)

    def test_date_index_reads_garmin_biometrics_metadata_date(self):
        result = ge.date_index([
            {"metaData": {"calendarDate": "2026-11-06T09:01:20.0"},
             "weight": {"weight": 84300.0, "sourceType": "MANUAL"}}
        ])
        self.assertIn("2026-11-06", result["by_date"])
        self.assertEqual(result["by_date"]["2026-11-06"]["weight"]["weight"], 84300.0)

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
        fitness_age_calls = [label for label, _ in cli.calls if label.startswith("fitness-age ")]
        self.assertEqual(len(fitness_age_calls), 30)
        self.assertIn("fitness_age", result["by_date"]["2026-11-07"])

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


    def test_hrv_summaries_are_clipped_to_requested_window(self):
        health = {
            "range_data": {
                "hrv_summaries": {
                    "raw": {
                        "hrvSummaries": [
                            {"calendarDate": "2026-10-13", "lastNightAvg": 61},
                            {"calendarDate": "2026-10-29", "lastNightAvg": 62},
                            {"calendarDate": "2026-11-11", "lastNightAvg": 63},
                        ]
                    },
                    "by_date": {},
                }
            },
            "by_date": {},
        }
        rows = me.health_records(
            "health", health, ge.date(2026, 10, 29), ge.date(2026, 11, 11))
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            [x["calendarDate"] for x in rows[0]["source_payload"]["hrvSummaries"]],
            ["2026-10-29", "2026-11-11"],
        )

    def test_optional_daily_treats_garmin_400_as_unavailable(self):
        class Http400CLI:
            def run(self, label, *args):
                raise RuntimeError("HTTP 400 Bad Request")

        unavailable = []
        ok, value = ge.optional_daily(Http400CLI(), unavailable, "steps 2011-08-01",
                                      "steps", "--date", "2011-08-01")
        self.assertTrue(ok)
        self.assertIsNone(value)
        self.assertEqual(len(unavailable), 1)
        self.assertIn("HTTP 400", unavailable[0]["reason"])

    def test_optional_daily_recognizes_http_404(self):
        class Http404CLI:
            def run(self, label, *args):
                raise RuntimeError("HTTP 404 Not Found")

        unavailable = []
        ok, value = ge.optional_daily(Http404CLI(), unavailable, "sleep 2021-08-01",
                                      "sleep", "--date", "2021-08-01")
        self.assertTrue(ok)
        self.assertIsNone(value)
        self.assertEqual(unavailable, [])

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
