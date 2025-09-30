USE SCHEMA health_experiments.stage;

CREATE OR REPLACE VIEW stage.supplement_usage_clean AS
SELECT
    user_id,
    event_date,
    supplement_name,
    dosage,
    CASE 
        WHEN dosage_unit = 'mg' THEN dosage / 1000
        ELSE dosage
    END AS dosage_grams,
    dosage_unit,
    CASE 
        WHEN is_placebo IN (TRUE, 'TRUE', 1) THEN TRUE
        WHEN is_placebo IN (FALSE, 'FALSE', 0) THEN FALSE
        ELSE NULL
    END AS is_placebo,
    experiment_id
FROM raw.supplement_usage;

select 'supplement_usage_clean (stage)', count(*) from stage.supplement_usage_clean;
