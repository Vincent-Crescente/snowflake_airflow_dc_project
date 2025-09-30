USE SCHEMA health_experiments.stage;

CREATE OR REPLACE VIEW stage.health_user_exp AS
SELECT
    uh.user_id,
    uh.event_date,
    uh.average_heart_rate,
    uh.average_glucose,
    uh.sleep_hours,
    uh.activity_level,
    su.supplement_name,
    su.dosage_grams,
    su.is_placebo,
    e.experiment_name,
    up.email,
    up.user_age_group
FROM stage.user_health_data_clean uh
LEFT JOIN stage.supplement_usage_clean su
    ON uh.user_id = su.user_id
   AND uh.event_date = su.event_date
LEFT JOIN stage.experiments_clean e
    ON su.experiment_id = e.experiment_id
LEFT JOIN stage.user_profiles_clean up
    ON uh.user_id = up.user_id;

select 'health_user_exp (stage - JOIN)', count(*) from stage.health_user_exp;