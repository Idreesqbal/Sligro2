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

looker_activity_history AS (
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
        history_status,
        inserted_date
    FROM {{ source("looker_meta_data", "looker_system_activity__history") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY inserted_date, query_id) = 1
)

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
    history_status,
    inserted_date
FROM looker_activity_history
