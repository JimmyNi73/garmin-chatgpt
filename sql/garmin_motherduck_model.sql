-- Garmin -> MotherDuck relational model
-- RAW stores one complete source record per natural record key. No month/file partitioning.
-- Apply after raw.garmin_payloads has been created by src/motherduck_loader.py.

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- One row per Garmin activity. Keep the original record available for fields
-- whose Garmin naming/availability differs between device generations.
CREATE OR REPLACE VIEW core.activities AS
WITH src AS (
    SELECT record_key, payload, source_run_id, loaded_at
    FROM raw.garmin_payloads
    WHERE source_name = 'activities'
)
SELECT
    coalesce(json_extract_string(payload, '$.summary.activityId'),
             json_extract_string(payload, '$.activityId'), record_key) AS activity_id,
    try_cast(substr(coalesce(
        json_extract_string(payload, '$.summary.startTimeLocal'),
        json_extract_string(payload, '$.summary.startTimeGMT'),
        json_extract_string(payload, '$.summary.beginTimestamp'),
        json_extract_string(payload, '$.startTimeLocal')
    ), 1, 10) AS DATE) AS activity_date,
    coalesce(json_extract_string(payload, '$.summary.activityType.typeKey'),
             json_extract_string(payload, '$.summary.activityType.typeName'),
             json_extract_string(payload, '$.summary.activityType')) AS activity_type,
    coalesce(json_extract_string(payload, '$.summary.activityName'),
             json_extract_string(payload, '$.detail.activityName')) AS activity_name,
    try_cast(json_extract_string(payload, '$.summary.distance') AS DOUBLE) AS distance_m,
    try_cast(json_extract_string(payload, '$.summary.duration') AS DOUBLE) AS duration_s,
    try_cast(json_extract_string(payload, '$.summary.elapsedDuration') AS DOUBLE) AS elapsed_duration_s,
    try_cast(json_extract_string(payload, '$.summary.averageSpeed') AS DOUBLE) AS average_speed_m_s,
    try_cast(json_extract_string(payload, '$.summary.averageHR') AS DOUBLE) AS average_hr,
    try_cast(json_extract_string(payload, '$.summary.maxHR') AS DOUBLE) AS max_hr,
    try_cast(json_extract_string(payload, '$.summary.calories') AS DOUBLE) AS calories,
    try_cast(json_extract_string(payload, '$.summary.averageRunCadence') AS DOUBLE) AS average_run_cadence,
    try_cast(json_extract_string(payload, '$.summary.maxRunCadence') AS DOUBLE) AS max_run_cadence,
    try_cast(json_extract_string(payload, '$.summary.vO2MaxValue') AS DOUBLE) AS vo2_max,
    coalesce(json_extract_string(payload, '$.summary.deviceId'),
             json_extract_string(payload, '$.device.deviceId')) AS device_id,
    payload AS source_payload,
    source_run_id, loaded_at
FROM src;

-- Preserve workout structures independently; activity id remains the parent key.
CREATE OR REPLACE VIEW core.activity_workouts AS
SELECT
    coalesce(json_extract_string(p.payload, '$.summary.activityId'),
             json_extract_string(p.payload, '$.activityId')) AS activity_id,
    w.key AS workout_index,
    w.value AS workout_payload,
    p.source_run_id, p.loaded_at
FROM raw.garmin_payloads AS p,
     LATERAL json_each(p.payload, '$.workouts') AS w
WHERE p.source_name = 'activities'
  AND json_type(json_extract(p.payload, '$.workouts')) = 'ARRAY';

-- Generic source-grain health records. This avoids multiplying daily measures
-- when joining them to multiple activities on the same day.
CREATE OR REPLACE VIEW core.health_records AS
SELECT source_name AS metric_source,
       record_key,
       event_date AS metric_date,
       payload AS source_payload,
       source_run_id, loaded_at
FROM raw.garmin_payloads
WHERE source_name NOT IN ('activities', 'gear_registry', 'devices');

CREATE OR REPLACE VIEW core.weight AS
SELECT record_key, event_date AS measurement_date,
       try_cast(coalesce(json_extract_string(payload, '$.weight'),
                         json_extract_string(payload, '$.weightInGrams')) AS DOUBLE) AS weight_value,
       payload AS source_payload
FROM raw.garmin_payloads
WHERE source_name = 'weight_measurements';

CREATE OR REPLACE VIEW core.sleep AS
SELECT record_key, event_date AS sleep_date, source_name AS sleep_source,
       payload AS source_payload
FROM raw.garmin_payloads
WHERE source_name IN ('sleep_summaries', 'sleep_daily');

CREATE OR REPLACE VIEW core.hrv AS
SELECT record_key, event_date AS measurement_date, source_name AS hrv_source,
       payload AS source_payload
FROM raw.garmin_payloads
WHERE source_name IN ('hrv_summaries', 'hrv_daily');

CREATE OR REPLACE VIEW core.vo2_max AS
SELECT activity_id, activity_date AS measurement_date, vo2_max,
       'activity_summary' AS measurement_source
FROM core.activities
WHERE vo2_max IS NOT NULL;

-- Gear is loaded only from retained ZIP/Dropbox source data; no undocumented
-- Garmin gear endpoint is called by this model or its loader.
CREATE OR REPLACE VIEW core.gear AS
SELECT record_key AS gear_key,
       coalesce(json_extract_string(payload, '$.gearPk'),
                json_extract_string(payload, '$.gearId'),
                json_extract_string(payload, '$.gear_id'),
                record_key) AS gear_id,
       coalesce(json_extract_string(payload, '$.customMakeModel'),
                json_extract_string(payload, '$.name'),
                json_extract_string(payload, '$.displayName')) AS gear_name,
       payload AS source_payload
FROM raw.garmin_payloads
WHERE source_name = 'gear_registry';

CREATE OR REPLACE VIEW core.activity_gear AS
SELECT activity_id,
       coalesce(json_extract_string(source_payload, '$.gear.gearPk'),
                json_extract_string(source_payload, '$.gear.gearId'),
                json_extract_string(source_payload, '$.gear.gear_id'),
                json_extract_string(source_payload, '$.summary.gearId')) AS gear_id
FROM core.activities
WHERE coalesce(json_extract_string(source_payload, '$.gear.gearPk'),
               json_extract_string(source_payload, '$.gear.gearId'),
               json_extract_string(source_payload, '$.gear.gear_id'),
               json_extract_string(source_payload, '$.summary.gearId')) IS NOT NULL;

CREATE OR REPLACE VIEW mart.running_performance AS
SELECT activity_id, activity_date, activity_name, activity_type,
       distance_m, duration_s,
       CASE WHEN distance_m > 0 THEN duration_s / (distance_m / 1000.0) END AS pace_sec_per_km,
       average_speed_m_s, average_hr, max_hr, average_run_cadence, vo2_max
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

-- Do not join core.health_records directly to activities without first
-- aggregating/deduplicating by metric source + date + record key.
