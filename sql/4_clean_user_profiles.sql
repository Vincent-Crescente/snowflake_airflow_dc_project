USE SCHEMA health_experiments.stage;


-- Cleaning user profiles
CREATE OR REPLACE VIEW stage.user_profiles_clean AS
(
    SELECT 
        user_id,
        email,
        COALESCE(age, 0) AS age,
        CASE
            WHEN age IS NULL THEN 'Unknown'
            WHEN age < 18 THEN 'Under 18'
            WHEN age BETWEEN 18 AND 25 THEN '18-25'
            WHEN age BETWEEN 26 AND 35 THEN '26-35'
            WHEN age BETWEEN 36 AND 45 THEN '36-45'
            WHEN age BETWEEN 46 AND 55 THEN '46-55'
            WHEN age BETWEEN 56 AND 65 THEN '56-65'
            ELSE 'Over 65'
        END AS user_age_group
    FROM raw.user_profiles
);

select 'user_profiles_clean (stage)', count(*) from stage.user_profiles_clean;
