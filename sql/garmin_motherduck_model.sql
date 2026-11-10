-- Garmin relational model.
-- Physical CORE tables are populated directly by src/motherduck_loader.py.
-- JSON is retained only as row-level source lineage, not as the analytical model.

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

-- Source-specific access paths retain original metric grain and do not fan out
-- when joined to activities. The JSON value preserves Garmin's changing schemas.
CREATE OR REPLACE VIEW core.health_records AS
SELECT metric_source, record_key, metric_date, metric_value,
       source_payload, source_run_id, loaded_at
FROM core.daily_metrics;

CREATE OR REPLACE VIEW core.weight AS
SELECT record_key, metric_date AS measurement_date,
       try_cast(coalesce(json_extract_string(metric_value, '$.weight'),
                         json_extract_string(metric_value, '$.weightInGrams'),
                         json_extract_string(metric_value, '$.weightKg')) AS DOUBLE) AS weight_value,
       metric_value AS source_value, source_payload
FROM core.daily_metrics
WHERE lower(metric_source) LIKE '%weight%';

CREATE OR REPLACE VIEW core.sleep AS
SELECT record_key, metric_date AS sleep_date, metric_source AS sleep_source,
       metric_value, source_payload
FROM core.daily_metrics
WHERE lower(metric_source) LIKE '%sleep%';

CREATE OR REPLACE VIEW core.hrv AS
SELECT record_key, metric_date AS measurement_date, metric_source AS hrv_source,
       metric_value, source_payload
FROM core.daily_metrics
WHERE lower(metric_source) LIKE '%hrv%';

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

-- Keep same-day health samples separate: never join daily_metrics to activities
-- without first aggregating to the desired metric/date grain.
