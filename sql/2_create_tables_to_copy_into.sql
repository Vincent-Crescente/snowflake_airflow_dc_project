
CREATE OR REPLACE TABLE health_experiments.raw.experiments (

    experiment_id VARCHAR,
    name VARCHAR,
    description TEXT
    
);

CREATE OR REPLACE TABLE health_experiments.raw.user_profiles (

    user_id VARCHAR,
    email VARCHAR,
    age INTEGER
    
);


CREATE OR REPLACE TABLE health_experiments.raw.supplement_usage (

    user_id VARCHAR,
    event_date DATE,
    supplement_name VARCHAR,
    dosage FLOAT,
    dosage_unit VARCHAR,
    is_placebo BOOLEAN,
    experiment_id VARCHAR

);

CREATE OR REPLACE TABLE health_experiments.raw.user_health_data (

    user_id VARCHAR,
    event_date DATE,
    average_heart_rate float,
    average_glucose FLOAT,
    sleep_hours VARCHAR,
    activity_level integer

);
