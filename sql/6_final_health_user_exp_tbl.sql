CREATE OR REPLACE TABLE final.health_user_exp_tbl AS
SELECT
    user_id,
    event_date,
    email,
    user_age_group,
    experiment_name,
    COALESCE(supplement_name, 'No intake') AS supplement_name,
    dosage_grams,
    is_placebo,
    average_heart_rate,
    average_glucose,
    sleep_hours,
    activity_level
FROM stage.health_user_exp;

select 'health_user_exp (FINAL)', count(*) from final.health_user_exp_tbl;