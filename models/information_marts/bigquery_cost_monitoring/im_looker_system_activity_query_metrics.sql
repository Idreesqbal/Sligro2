{{
    config(
        materialized='incremental',
        unique_key='query_id',
        partition_by={
            "field": "inserted_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

WITH

looker_query_metrics AS (
    SELECT
        query_created_time,
        replace(query_metrics_bigquery_job_id, '"', '') AS bigquery_job_id,
        query_id,
        inserted_date
    FROM {{ source("looker_meta_data", "looker_system_activity__query_metrics") }}
    QUALIFY row_number() OVER (PARTITION BY inserted_date, query_created_time) = 1
)

SELECT
    query_created_time,
    bigquery_job_id,
    query_id,
    inserted_date
FROM looker_query_metrics
