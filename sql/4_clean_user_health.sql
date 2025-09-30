USE SCHEMA health_experiments.stage;


-- Cleaning user user_health_data
CREATE OR REPLACE VIEW stage.user_health_data_clean AS
SELECT
    user_id,
    event_date,
    average_heart_rate AS average_heart_rate,
    average_glucose AS average_glucose,
    REGEXP_REPLACE(sleep_hours, '[hH]', '') AS sleep_hours,
    CASE 
        WHEN activity_level IS NULL THEN 0
        WHEN activity_level < 0 THEN 0
        WHEN activity_level > 100 THEN 100
        ELSE activity_level
    END AS activity_level
FROM raw.user_health_data
WHERE activity_level IS NULL
   OR (activity_level >= 0 AND activity_level <= 100);


select 'user_health_data_clean (stage)', count(*) from stage.user_health_data_clean;



