{{
    config(
        materialized='incremental',
        unique_key='job_id',
        partition_by={
            "field": "date_time",
            "data_type": "timestamp",
            "granularity": "day"
        }
    )
}}

WITH

{% if is_incremental() %}
    max_date_time_cte AS (
        SELECT MAX(date_time) AS max_date_time
        FROM {{ this }}
    ),
{% endif %}

bigquery_jobs AS (
    SELECT
        project_id,
        user_email,
        "{{ env_var('DBT_DATA_WAREHOUSE_PROJECT_ID') }}" AS dwh_project_id,
        job_id,
        DATETIME(creation_time) AS date_time,
        job_type,
        cache_hit,
        statement_type,
        priority,
        query,
        state,
        total_bytes_processed,
        total_bytes_billed
    FROM `{{ env_var('DBT_DATA_WAREHOUSE_PROJECT_ID') }}.region-europe-west4.INFORMATION_SCHEMA.JOBS`
),

bigquery_jobs_prepared AS (
    SELECT
        bq.project_id,
        bq.user_email,
        SPLIT(bq.user_email, "@")[OFFSET(0)] AS user_name,
        CONCAT(bq.dwh_project_id, ":europe-west4.", bq.job_id) AS job_id,
        bq.date_time,
        bq.job_type,
        bq.cache_hit,
        bq.statement_type,
        bq.priority,
        bq.query,
        bq.state,
        bq.total_bytes_processed,
        bq.total_bytes_billed,
        -- TODO: Make these magic numbers configurable and add a config file (1)
        (bq.total_bytes_billed / 1099511627776 * 6.934125) AS analysis_costs_eur
    FROM bigquery_jobs AS bq
    {% if is_incremental() %}
        , max_date_time_cte AS mdt -- noqa
    {% endif %}
    WHERE
        {% if is_incremental() %}
            bq.date_time > mdt.max_date_time
        {% else %}
            DATETIME(bq.date_time) > '2024-01-01'
        {% endif %}
)

SELECT
    project_id,
    user_email,
    user_name,
    job_id,
    date_time,
    job_type,
    cache_hit,
    statement_type,
    priority,
    query,
    state,
    total_bytes_processed,
    total_bytes_billed,
    analysis_costs_eur
FROM bigquery_jobs_prepared
