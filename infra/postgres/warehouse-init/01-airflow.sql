\if :{?airflow_db_password}
\else
    \echo 'ERROR: airflow_db_password is required'
    \quit 1
\endif

SELECT format(
    'CREATE ROLE airflow_user LOGIN PASSWORD %L',
    :'airflow_db_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles
    WHERE rolname = 'airflow_user'
)
\gexec

SELECT 'CREATE DATABASE airflow OWNER airflow_user'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database
    WHERE datname = 'airflow'
)
\gexec
