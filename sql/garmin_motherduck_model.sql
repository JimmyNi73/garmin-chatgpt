-- Garmin relational model. RAW retains each complete source record, not monthly files.
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW core.activities AS
SELECT
  coalesce(json_extract_string(payload, '$.summary.activityId'),
           json_extract_string(payload, '$.activityId')) AS activity_id,
  try_cast(substr(coalesce(
    json_extract_string(payload, '$.summary.startTimeLocal'),
    json_extract_string(payload, '$.summary.startTimeGMT'),
    json_extract_string(payload, '$.startTimeLocal'),
    json_extract_string(payload, '$.calendarDate')
  ), 1, 10) AS DATE) AS activity_date,
  coalesce(json_extract_string(payload, '$.summary.activityType.typeKey'),
           json_extract_string(payload, '$.summary.activityType.typeName')) AS activity_type,
  coalesce(json_extract_string(payload, '$.summary.activityName'),
           json_extract_string(payload, '$.detail.activityName')) AS activity_name,
  try_cast(json_extract_string(payload, '$.summary.distance') AS DOUBLE) AS distance_m,
  try_cast(json_extract_string(payload, '$.summary.duration') AS DOUBLE) AS duration_s,
  try_cast(json_extract_string(payload, '$.summary.averageSpeed') AS DOUBLE) AS average_speed_m_s,
  try_cast(json_extract_string(payload, '$.summary.averageHR') AS DOUBLE) AS average_hr,
  try_cast(json_extract_string(payload, '$.summary.maxHR') AS DOUBLE) AS max_hr,
  try_cast(json_extract_string(payload, '$.summary.calories') AS DOUBLE) AS calories,
  try_cast(json_extract_string(payload, '$.summary.averageRunCadence') AS DOUBLE) AS average_run_cadence,
  try_cast(json_extract_string(payload, '$.summary.vO2MaxValue') AS DOUBLE) AS vo2_max,
  try_cast(json_extract_string(payload, '$.gear.gear_id') AS BIGINT) AS gear_id,
  payload AS activity_payload,
  source_run_id, loaded_at
FROM raw.garmin_payloads
WHERE source_name = 'activities';

CREATE OR REPLACE VIEW core.daily_health AS
SELECT source_name AS metric_type, event_date AS health_date,
       payload AS metric_payload, source_run_id, loaded_at
FROM raw.garmin_payloads
WHERE source_name IN (
  'steps','stress','weight_measurements','sleep_summaries','sleep_daily',
  'body_battery','daily_summaries','hrv_summaries','hrv_daily',
  'heart_rate_daily','spo2_daily','fitness_age_daily','wellness_activity_summaries'
);

CREATE OR REPLACE VIEW core.weight AS
SELECT event_date AS measurement_date,
       try_cast(coalesce(json_extract_string(payload, '$.weight'),
                         json_extract_string(payload, '$.weightInGrams')) AS DOUBLE) AS weight_value,
       payload AS measurement_payload
FROM raw.garmin_payloads
WHERE source_name = 'weight_measurements';

CREATE OR REPLACE VIEW core.sleep AS
SELECT event_date AS sleep_date, payload AS sleep_payload
FROM raw.garmin_payloads
WHERE source_name IN ('sleep_summaries','sleep_daily');

CREATE OR REPLACE VIEW core.hrv AS
SELECT event_date AS measurement_date, payload AS hrv_payload
FROM raw.garmin_payloads
WHERE source_name IN ('hrv_summaries','hrv_daily');

CREATE OR REPLACE VIEW core.vo2_max AS
SELECT activity_id, activity_date AS measurement_date, vo2_max,
       'activity_summary' AS measurement_source
FROM core.activities
WHERE vo2_max IS NOT NULL;

CREATE OR REPLACE VIEW core.gear AS
SELECT DISTINCT
  try_cast(json_extract_string(payload, '$.gear_id') AS BIGINT) AS gear_id,
  coalesce(json_extract_string(payload, '$.customMakeModel'),
           json_extract_string(payload, '$.name')) AS gear_name,
  payload AS gear_payload
FROM raw.garmin_payloads
WHERE source_name = 'gear_registry';

CREATE OR REPLACE VIEW core.activity_gear AS
SELECT activity_id, gear_id
FROM core.activities
WHERE activity_id IS NOT NULL AND gear_id IS NOT NULL;

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
SELECT health_date, metric_type, metric_payload
FROM core.daily_health;
