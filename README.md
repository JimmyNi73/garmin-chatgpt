# Garmin data pipeline

Production rolling refresh: last 30 days, upsert-only, scheduled daily.

## Sources and destinations

- Garmin Personal API is the only source for MotherDuck ingestion. Dropbox ZIP files and archive JSON files are not read by either MotherDuck workflow.
- `.github/workflows/garmin-export.yml` is the separate scheduled archive export to Dropbox. It refreshes the rolling 30-day activity/health window and maintains monthly History/Current JSON files at 06:00, 08:00, 14:00, 20:00 and 22:00 Europe/Stockholm.
- `.github/workflows/garmin-motherduck-full.yml` performs a full Garmin-to-MotherDuck rebuild.
- `.github/workflows/garmin-motherduck.yml` is a manually confirmed, bounded Garmin-only test. It does not read ZIP or Dropbox data.

## MotherDuck model

The loader writes every fetched source record to both `raw.garmin_payloads` and a source-specific table such as `raw.activities`, `raw.sleep`, `raw.heart_rate`, `raw.hydration` and `raw.food_logs`. Full Garmin response envelopes and month-level source availability metadata are retained in RAW.

Activities are stored together in `raw.activities` and curated in `core.activities`. The complete source activity JSON is retained in `core.activities.source_payload`; workouts, devices and gear are also modeled separately. Health measurements are curated in `core.daily_metrics`, while the complete original source records remain in RAW. SQL views publish canonical sleep, weight, HRV, VO2 max and training-performance models.

The supported ingestion set includes activity summaries/details/workouts; stress, steps, hydration, weight measurements/goals, sleep summaries, body battery, daily summaries, HRV summaries and wellness activity summaries; daily sleep, heart rate, HRV, SpO2, fitness age and food logs; plus profile settings, heart-rate zones, gear and device metadata. VO2 max is retained when supplied in activity summaries.

Training Readiness is not exposed by the documented Garmin Personal API endpoint set or the supported `gc-personal` CLI. It is explicitly recorded as unsupported in monthly source-availability metadata rather than silently represented as zero or inferred.

## Batch and restart behavior

- MotherDuck batch size is 100 records.
- Every committed health batch has a content-hash checkpoint. Activity batches checkpoint their activity IDs only after activity, workout and device writes have committed.
- Checkpoints are stored in `raw.garmin_ingestion_progress` and scoped to the GitHub Actions run ID.
- A rerun of the same GitHub Actions run ID preserves already committed RAW/CORE data and resumes from checkpoints. A new full run truncates physical RAW, CORE and MART tables once, then starts a clean rebuild.
- The workflow validates Garmin activity IDs, month and activity checkpoints, source-specific RAW versus generic RAW payload parity, and complete activity payload preservation from RAW into CORE before reporting success.
- Transient daily-endpoint failures abort the month; they are not recorded as successful empty data. Garmin's documented no-data responses and historical HTTP 400 gaps are recorded separately as unavailable.

The full workflow validates Python syntax and the complete unit-test suite before the ingestion job can run. A normal code push does not start a full MotherDuck load; use manual workflow dispatch or the explicit `[run-garmin-full]` trigger.
