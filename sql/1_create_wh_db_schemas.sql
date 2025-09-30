
CREATE OR REPLACE WAREHOUSE experiments_1001
     WITH WAREHOUSE_SIZE = 'XSMALL'
     AUTO_SUSPEND = 60      
     AUTO_RESUME = TRUE;     


CREATE OR REPLACE DATABASE health_experiments;

CREATE OR REPLACE SCHEMA health_experiments.raw;
CREATE OR REPLACE SCHEMA health_experiments.stage;
CREATE OR REPLACE SCHEMA health_experiments.final;


USE ROLE ACCOUNTADMIN;
USE WAREHOUSE experiments_1001;
USE DATABASE health_experiments;
USE SCHEMA health_experiments.raw;


CREATE OR REPLACE STAGE experiments_stage;


