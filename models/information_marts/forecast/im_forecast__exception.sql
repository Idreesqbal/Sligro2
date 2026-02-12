{{ config(
    partition_by={
        "field": "valid_from",
        "data_type": "TIMESTAMP",
        "granularity": "day"
    }
  )
}}

WITH masat__demand_forecast_exception AS (
    SELECT
        pk_exception_code,
        demand_forecast_hk,
        exception_type,
        exception_code,
        short_description,
        start_datetime,
        end_datetime,
        record_source
    FROM {{ ref('bv_masat__demand_forecast_exception') }}
),

hub_demand_forecast AS (
    SELECT
        demand_forecast_hk,
        demand_forecast_bk
    FROM {{ ref('bv_hub__demand_forecast') }}
),

joined AS (
    SELECT
        CONCAT(
            masat.pk_exception_code,
            "||",
            masat.start_datetime,
            "||",
            COALESCE(masat.end_datetime, {{ var('end_date') }})
        ) AS pk_exception_code,
        masat.pk_exception_code AS fk_exception_code,
        hub_df.demand_forecast_bk AS fk_demand_forecast_code,
        masat.start_datetime AS valid_from,
        masat.end_datetime AS valid_to,
        masat.exception_type,
        masat.short_description AS description,
        masat.record_source
    FROM masat__demand_forecast_exception AS masat
    LEFT JOIN hub_demand_forecast AS hub_df
        ON masat.demand_forecast_hk = hub_df.demand_forecast_hk
)

SELECT
    pk_exception_code,
    fk_exception_code,
    fk_demand_forecast_code,
    exception_type,
    description,
    "MISSING" AS exception_quantity,
    valid_from,
    valid_to,
    record_source
FROM joined
