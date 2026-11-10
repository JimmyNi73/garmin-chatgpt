-- Garmin analytical model for MotherDuck.
-- RAW monthly_exports retains the complete, original monthly JSON payload.
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW core.activities AS
SELECT
  json_extract_string(a.value, '$.summary.activityId') AS activity_id,
  try_cast(substr(coalesce(
    json_extract_string(a.value, '$.summary.startTimeLocal'),
    json_extract_string(a.value, '$.summary.startTimeGMT')
  ), 1, 10) AS DATE) AS activity_date,
  json_extract_string(a.value, '$.summary.activityType.typeKey') AS activity_type,
  coalesce(
    json_extract_string(a.value, '$.summary.activityName'),
    json_extract_string(a.value, '$.detail.activityName')
  ) AS activity_name,
  try_cast(json_extract_string(a.value, '$.summary.distance') AS DOUBLE) AS distance_m,
  try_cast(json_extract_string(a.value, '$.summary.duration') AS DOUBLE) AS duration_s,
  try_cast(json_extract_string(a.value, '$.summary.averageSpeed') AS DOUBLE) AS average_speed_m_s,
  try_cast(json_extract_string(a.value, '$.summary.averageHR') AS DOUBLE) AS average_hr,
  try_cast(json_extract_string(a.value, '$.summary.maxHR') AS DOUBLE) AS max_hr,
  try_cast(json_extract_string(a.value, '$.summary.calories') AS DOUBLE) AS calories,
  try_cast(json_extract_string(a.value, '$.summary.averageRunCadence') AS DOUBLE) AS average_run_cadence,
  try_cast(json_extract_string(a.value, '$.summary.vO2MaxValue') AS DOUBLE) AS vo2_max,
  try_cast(json_extract_string(a.value, '$.gear.gear_id') AS BIGINT) AS gear_id,
  a.value AS activity_payload,
  m.record_key AS source_month
FROM raw.garmin_payloads m,
     LATERAL json_each(m.payload, '$.activities') a
WHERE m.source_name = 'monthly_exports';

CREATE OR REPLACE VIEW core.gear AS
SELECT DISTINCT
  try_cast(json_extract_string(g.value, '$.gear_id') AS BIGINT) AS gear_id,
  coalesce(json_extract_string(g.value, '$.customMakeModel'),
           json_extract_string(g.value, '$.name')) AS gear_name,
  g.value AS gear_payload
FROM raw.garmin_payloads m,
     LATERAL json_each(m.payload, '$.gear_registry') g
WHERE m.source_name = 'monthly_exports';

CREATE OR REPLACE VIEW core.activity_gear AS
SELECT activity_id, gear_id
FROM core.activities
WHERE activity_id IS NOT NULL AND gear_id IS NOT NULL;

CREATE OR REPLACE VIEW core.daily_health AS
SELECT
  try_cast(d.key AS DATE) AS health_date,
  d.value AS daily_payload,
  m.record_key AS source_month
FROM raw.garmin_payloads m,
     LATERAL json_each(m.payload, '$.health.by_date') d
WHERE m.source_name = 'monthly_exports';

CREATE OR REPLACE VIEW core.daily_range_health AS
SELECT
  try_cast(d.key AS DATE) AS health_date,
  src.key AS metric_source,
  d.value AS metric_payload,
  m.record_key AS source_month
FROM raw.garmin_payloads m,
     LATERAL json_each(m.payload, '$.health.range_data') src,
     LATERAL json_each(src.value, '$.by_date') d
WHERE m.source_name = 'monthly_exports';

CREATE OR REPLACE VIEW core.vo2_max AS
SELECT activity_id, activity_date AS measurement_date, vo2_max,
       'activity_summary' AS measurement_source
FROM core.activities
WHERE vo2_max IS NOT NULL;

CREATE OR REPLACE VIEW mart.running_performance AS
SELECT activity_id, activity_date, activity_name, activity_type,
       distance_m, duration_s,
       CASE WHEN distance_m > 0 THEN duration_s / (distance_m / 1000.0) END AS pace_sec_per_km,
       average_speed_m_s, average_hr, max_hr, average_run_cadence,
       vo2_max, gear_id
FROM core.activities
WHERE lower(coalesce(activity_type, '')) LIKE '%running%'
   OR lower(coalesce(activity_type, '')) LIKE '%run%';

CREATE OR REPLACE VIEW mart.training_overview AS
SELECT activity_date, count(*) AS activity_count,
       sum(distance_m) AS total_distance_m,
       sum(duration_s) AS total_duration_s,
       sum(calories) AS total_activity_calories,
       count(*) FILTER (WHERE lower(coalesce(activity_type, '')) LIKE '%running%'
                          OR lower(coalesce(activity_type, '')) LIKE '%run%') AS running_sessions,
       count(*) FILTER (WHERE lower(coalesce(activity_type, '')) LIKE '%walking%'
                          OR lower(coalesce(activity_type, '')) LIKE '%walk%') AS walking_sessions
FROM core.activities
GROUP BY activity_date;

CREATE OR REPLACE VIEW mart.daily_recovery AS
SELECT health_date, daily_payload, source_month
FROM core.daily_health;
