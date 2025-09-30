from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator


# Default args
default_args = {
    'owner': 'vinny',
    'start_date': datetime(2025, 9, 29),
    'retries': 1,
}

# DAG definition
with DAG(
    dag_id='snowflake_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,  # manual trigger
    catchup=False,
    template_searchpath=["/home/vinny/snowflake_dc_project/sql"],
) as dag:

    # Step 1: Create warehouse, DB, schemas, stage
    create_wh_db_schemas = SQLExecuteQueryOperator(
        task_id='create_wh_db_schemas',
        conn_id='snowflake_default1',
        sql='1_create_wh_db_schemas.sql'
    )

    # Step 2: Create tables
    create_tables = SQLExecuteQueryOperator(
        task_id='create_tables',
        conn_id='snowflake_default1',
        sql='2_create_tables_to_copy_into.sql'
    )

    # Step 3: Load CSVs into raw tables
    copy_into_raw_tables = SQLExecuteQueryOperator(
        task_id='copy_into_raw_tables',
        conn_id='snowflake_default1',
        sql='3_copy_into_commands.sql'
    )

    # Step 4: Data cleaning and transformations
    clean_experiments = SQLExecuteQueryOperator(
        task_id='clean_experiments',
        conn_id='snowflake_default1',
        sql='4_clean_experiments.sql'
    )

    clean_supplement_usage = SQLExecuteQueryOperator(
        task_id='clean_supplement_usage',
        conn_id='snowflake_default1',
        sql='4_clean_supplement_usage.sql'
    )

    clean_user_health = SQLExecuteQueryOperator(
        task_id='clean_user_health',
        conn_id='snowflake_default1',
        sql='4_clean_user_health.sql'
    )

    clean_user_profiles = SQLExecuteQueryOperator(
        task_id='clean_user_profiles',
        conn_id='snowflake_default1',
        sql='4_clean_user_profiles.sql'
    )

    # Step 5: Health supplement experiments (main join)
    health_supplement_experiments = SQLExecuteQueryOperator(
        task_id='health_supplement_experiments',
        conn_id='snowflake_default1',
        sql='5_health_supp_exp.sql'
    )

    # Step 6: Final health user experiments table
    final_health_user_exp_tbl = SQLExecuteQueryOperator(
        task_id='final_health_user_exp_tbl',
        conn_id='snowflake_default1',
        sql='6_final_health_user_exp_tbl.sql'
    )

    # Task dependencies
    create_wh_db_schemas >> create_tables >> copy_into_raw_tables
    copy_into_raw_tables >> [clean_experiments, clean_supplement_usage, clean_user_health, clean_user_profiles]
    [clean_experiments, clean_supplement_usage, clean_user_health, clean_user_profiles] >> health_supplement_experiments >> final_health_user_exp_tbl
