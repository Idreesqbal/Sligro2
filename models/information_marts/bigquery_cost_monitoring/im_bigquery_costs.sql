{{
    config(
        materialized='incremental',
        unique_key='query_id',
        partition_by={
            "field": "query_created_time",
            "data_type": "timestamp",
            "granularity": "day"
        }
    )
}}

WITH
query_metrics AS (
    SELECT
        query_created_time,
        bigquery_job_id,
        query_id,
        inserted_date
    FROM {{ ref('im_looker_system_activity_query_metrics') }}
),

activity_history AS (
    SELECT
        look_id,
        look_title,
        query_id,
        query_model,
        query_created_date,
        dashboard_id,
        dashboard_is_legacy,
        dashboard_title,
        user_email,
        user_name,
        query_formatted_pivots,
        dashboard_creator_name,
        history_cache,
        history_completed_date,
        history_connection_name,
        history_created_time,
        query_view,
        query_created_time,
        history_runtime,
        history_most_recent_run_at_time,
        history_most_recent_length,
        history_message,
        history_status
    FROM {{ ref('im_looker_system_activity_history') }}
),

information_schema AS (
    SELECT
        bq.project_id,
        bq.user_email,
        bq.user_name,
        bq.job_id,
        bq.date_time,
        bq.job_type,
        bq.cache_hit,
        bq.statement_type,
        bq.priority,
        bq.query,
        bq.state,
        bq.total_bytes_processed,
        bq.total_bytes_billed,
        bq.analysis_costs_eur
    FROM {{ ref('im_bigquery_jobs') }} AS bq
)

SELECT
    qm.query_created_time,
    qm.query_id,
    ah.look_id,
    ah.look_title,
    ah.query_model,
    ah.query_created_date,
    ah.dashboard_id,
    ah.dashboard_is_legacy,
    ah.dashboard_title,
    ah.user_email,
    ah.user_name,
    ah.query_formatted_pivots,
    ah.dashboard_creator_name,
    ah.history_cache,
    ah.history_connection_name,
    ah.query_view,
    ah.history_runtime,
    ah.history_message,
    ah.history_status,
    ish.project_id,
    ish.user_email AS looker_service_account_email,
    ish.user_name AS looker_service_account_name,
    ish.job_type,
    ish.cache_hit,
    ish.statement_type,
    ish.priority,
    ish.query,
    ish.state,
    ish.total_bytes_processed,
    ish.total_bytes_billed,
    ish.analysis_costs_eur
FROM query_metrics AS qm
LEFT JOIN activity_history AS ah ON qm.query_id = ah.query_id
LEFT JOIN information_schema AS ish ON qm.bigquery_job_id = ish.job_id
WHERE ish.project_id IS NOT null
