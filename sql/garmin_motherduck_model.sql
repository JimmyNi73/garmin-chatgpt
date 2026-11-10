-- Garmin relational model.
-- Physical CORE tables are populated directly by src/motherduck_loader.py.
-- JSON is retained as source lineage; source-specific views expose typed measures.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS core.activities (
    activity_id VARCHAR PRIMARY KEY, activity_date DATE, activity_type VARCHAR,
    activity_name VARCHAR, distance_m DOUBLE, duration_s DOUBLE,
    elapsed_duration_s DOUBLE, average_speed_m_s DOUBLE,
    average_hr DOUBLE, max_hr DOUBLE, calories DOUBLE,
    average_run_cadence DOUBLE, max_run_cadence DOUBLE, vo2_max DOUBLE,
    device_id VARCHAR, gear_id VARCHAR, source_payload JSON NOT NULL,
    source_run_id VARCHAR NOT NULL, loaded_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS core.activity_workouts (
    activity_id VARCHAR NOT NULL, workout_index INTEGER NOT NULL,
    workout_payload JSON NOT NULL, source_run_id VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (activity_id, workout_index)
);
CREATE TABLE IF NOT EXISTS core.daily_metrics (
    metric_source VARCHAR NOT NULL, record_key VARCHAR NOT NULL,
    metric_date DATE, metric_value JSON, source_payload JSON NOT NULL,
    source_run_id VARCHAR NOT NULL, loaded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_source, record_key)
);
CREATE TABLE IF NOT EXISTS core.devices (
    device_id VARCHAR PRIMARY KEY, device_model VARCHAR,
    source_payload JSON NOT NULL, source_run_id VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS core.gear (
    gear_id VARCHAR PRIMARY KEY, gear_name VARCHAR,
    source_payload JSON NOT NULL, source_run_id VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL
);

CREATE OR REPLACE VIEW core.health_records AS
SELECT metric_source, record_key, metric_date, metric_value,
       source_payload, source_run_id, loaded_at
FROM core.daily_metrics;

-- Garmin weight_measurements may expose weight as a scalar (grams) or as the
-- nested weight object used by Garmin's biometrics export. Keep individual
-- same-day measurements distinct using the stable record_key.
CREATE OR REPLACE VIEW core.weight AS
WITH parsed AS (
    SELECT *,
        coalesce(
            try_cast(json_extract_string(metric_value, '$.weight') AS DOUBLE),
            try_cast(json_extract_string(metric_value, '$.weight.weight') AS DOUBLE),
            try_cast(json_extract_string(metric_value, '$.weightInGrams') AS DOUBLE),
            try_cast(json_extract_string(metric_value, '$.weight.weightInGrams') AS DOUBLE)
        ) AS weight_grams,
        coalesce(
            json_extract_string(metric_value, '$.timestampGMT'),
            json_extract_string(metric_value, '$.weight.timestampGMT')
        ) AS weight_timestamp
    FROM core.daily_metrics
    WHERE lower(metric_source) LIKE '%weight%'
)
SELECT
    record_key,
    coalesce(metric_date,
             try_cast(try_cast(weight_timestamp AS TIMESTAMPTZ) AS DATE),
             try_cast(json_extract_string(metric_value, '$.calendarDate') AS DATE)) AS measurement_date,
    try_cast(json_extract_string(metric_value, '$.from') AS BIGINT) AS period_start_ms,
    try_cast(json_extract_string(metric_value, '$.until') AS BIGINT) AS period_end_ms,
    try_cast(weight_timestamp AS TIMESTAMPTZ) AS measurement_timestamp,
    CASE
        WHEN weight_grams IS NOT NULL THEN weight_grams / 1000.0
        ELSE coalesce(
            try_cast(json_extract_string(metric_value, '$.weightKg') AS DOUBLE),
            try_cast(json_extract_string(metric_value, '$.weight.weightKg') AS DOUBLE)
        )
    END AS weight_kg,
    coalesce(
        try_cast(json_extract_string(metric_value, '$.bmi') AS DOUBLE),
        try_cast(json_extract_string(metric_value, '$.weight.bmi') AS DOUBLE)
    ) AS bmi,
    coalesce(
        try_cast(json_extract_string(metric_value, '$.bodyFat') AS DOUBLE),
        try_cast(json_extract_string(metric_value, '$.weight.bodyFat') AS DOUBLE)
    ) AS body_fat_pct,
    coalesce(
        try_cast(json_extract_string(metric_value, '$.bodyWater') AS DOUBLE),
        try_cast(json_extract_string(metric_value, '$.weight.bodyWater') AS DOUBLE)
    ) AS body_water_pct,
    coalesce(
        try_cast(json_extract_string(metric_value, '$.muscleMass') AS DOUBLE),
        try_cast(json_extract_string(metric_value, '$.weight.muscleMass') AS DOUBLE)
    ) / 1000.0 AS muscle_mass_kg,
    coalesce(
        try_cast(json_extract_string(metric_value, '$.boneMass') AS DOUBLE),
        try_cast(json_extract_string(metric_value, '$.weight.boneMass') AS DOUBLE)
    ) / 1000.0 AS bone_mass_kg,
    coalesce(
        try_cast(json_extract_string(metric_value, '$.visceralFat') AS DOUBLE),
        try_cast(json_extract_string(metric_value, '$.weight.visceralFat') AS DOUBLE)
    ) AS visceral_fat,
    metric_value AS source_value,
    source_payload
FROM parsed
WHERE weight_grams IS NOT NULL
   OR json_extract(metric_value, '$.weightKg') IS NOT NULL
   OR json_extract(metric_value, '$.weight.weightKg') IS NOT NULL;

CREATE OR REPLACE VIEW core.sleep AS
SELECT record_key, metric_date AS sleep_date, metric_source AS sleep_source,
       metric_value, source_payload
FROM core.daily_metrics
WHERE lower(metric_source) LIKE '%sleep%';

-- HRV source rows are daily summaries. Extract the actual nightly/weekly values,
-- status and Garmin baseline rather than forcing consumers to parse JSON.
CREATE OR REPLACE VIEW core.hrv AS
SELECT
    record_key,
    coalesce(metric_date, try_cast(json_extract_string(metric_value, '$.hrvSummary.calendarDate') AS DATE),
             try_cast(json_extract_string(metric_value, '$.calendarDate') AS DATE)) AS measurement_date,
    metric_source AS hrv_source,
    try_cast(json_extract_string(metric_value, '$.hrvSummary.lastNightAvg') AS DOUBLE) AS last_night_avg_ms,
    try_cast(json_extract_string(metric_value, '$.hrvSummary.lastNight5MinHigh') AS DOUBLE) AS last_night_5min_high_ms,
    try_cast(json_extract_string(metric_value, '$.hrvSummary.weeklyAvg') AS DOUBLE) AS weekly_avg_ms,
    json_extract_string(metric_value, '$.hrvSummary.status') AS hrv_status,
    try_cast(json_extract_string(metric_value, '$.hrvSummary.baseline.balancedLow') AS DOUBLE) AS baseline_balanced_low_ms,
    try_cast(json_extract_string(metric_value, '$.hrvSummary.baseline.balancedUpper') AS DOUBLE) AS baseline_balanced_upper_ms,
    metric_value AS source_value,
    source_payload
FROM core.daily_metrics
WHERE lower(metric_source) LIKE '%hrv%'
  AND json_extract(metric_value, '$.hrvSummary') IS NOT NULL;

CREATE OR REPLACE VIEW core.vo2_max AS
SELECT activity_id, activity_date AS measurement_date, vo2_max,
       'activity_summary' AS measurement_source
FROM core.activities
WHERE vo2_max IS NOT NULL
UNION ALL
SELECT record_key, metric_date,
       try_cast(coalesce(json_extract_string(metric_value, '$.vo2Max'),
                         json_extract_string(metric_value, '$.vo2MaxValue'),
                         json_extract_string(metric_value, '$.value')) AS DOUBLE),
       metric_source
FROM core.daily_metrics
WHERE lower(metric_source) LIKE '%vo2%'
  AND coalesce(json_extract_string(metric_value, '$.vo2Max'),
               json_extract_string(metric_value, '$.vo2MaxValue'),
               json_extract_string(metric_value, '$.value')) IS NOT NULL;

CREATE OR REPLACE VIEW mart.running_performance AS
SELECT activity_id, activity_date, activity_name, activity_type,
       distance_m, duration_s,
       CASE WHEN distance_m > 0 THEN duration_s / (distance_m / 1000.0) END AS pace_sec_per_km,
       average_speed_m_s, average_hr, max_hr, average_run_cadence, vo2_max,
       device_id, gear_id
FROM core.activities
WHERE lower(coalesce(activity_type, '')) LIKE '%run%';

CREATE OR REPLACE VIEW mart.daily_training_load AS
SELECT activity_date,
       count(*) AS activity_count,
       sum(distance_m) AS total_distance_m,
       sum(duration_s) AS total_duration_s,
       sum(calories) AS total_activity_calories,
       count(*) FILTER (WHERE lower(coalesce(activity_type, '')) LIKE '%run%') AS running_sessions,
       count(*) FILTER (WHERE lower(coalesce(activity_type, '')) LIKE '%walk%') AS walking_sessions,
       count(*) FILTER (WHERE lower(coalesce(activity_type, '')) LIKE '%strength%'
                          OR lower(coalesce(activity_type, '')) LIKE '%training%') AS strength_or_training_sessions
FROM core.activities
GROUP BY activity_date;

-- Keep same-day health samples separate: aggregate to metric/date grain before joining.
