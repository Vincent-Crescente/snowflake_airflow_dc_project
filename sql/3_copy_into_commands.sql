USE WAREHOUSE experiments_1001;
USE DATABASE health_experiments;
USE SCHEMA raw;


-- 1. Load experiments.csv
COPY INTO experiments
FROM @experiments_stage/experiments.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
);

-- 2. Load user_profiles.csv
COPY INTO user_profiles
FROM @experiments_stage/user_profiles.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
);

-- 3. Load supplement_usage.csv
COPY INTO supplement_usage
FROM @experiments_stage/supplement_usage.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
);

-- 4. Load user_health_data.csv
COPY INTO user_health_data
FROM @experiments_stage/user_health_data.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
);


-- Verify counts 
SELECT 'experiments (RAW)' AS table_name, COUNT(*) AS row_count FROM experiments
UNION ALL
SELECT 'user_profiles', COUNT(*) FROM user_profiles
UNION ALL
SELECT 'supplement_usage (RAW)', COUNT(*) FROM supplement_usage
UNION ALL
SELECT 'user_health_data (RAW)', COUNT(*) FROM user_health_data;


