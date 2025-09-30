USE SCHEMA health_experiments.stage;

CREATE OR REPLACE VIEW stage.experiments_clean AS
SELECT
    experiment_id,
    name AS experiment_name,
    description
FROM raw.experiments;


select 'health_experiments_clean (stage)', count(*) from stage.experiments_clean;

