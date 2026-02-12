{{ config(
    partition_by={
      "field": "start_date",
      "data_type": "DATE"
    }
  )
}}

WITH hub_demand_forecast AS (
    SELECT
        demand_forecast_bk,
        demand_forecast_hk
    FROM {{ ref('bv_hub__demand_forecast') }}
),

lnk__demand_forecast__site_article AS (
    SELECT
        demand_forecast_hk,
        article_hk,
        site_hk
    FROM {{ ref('bv_lnk__demand_forecast__site_article') }}
),

lnk__demand_forecast__customer AS (
    SELECT
        demand_forecast_hk,
        customer_hk
    FROM {{ ref('bv_lnk__demand_forecast__customer') }}
),

hub_article AS (
    SELECT
        article_hk,
        article_bk
    FROM {{ ref('rv_hub__article_AS400') }}
),

hub_customer AS (
    SELECT
        customer_hk,
        customer_bk
    FROM {{ ref('rv_hub__customer') }}
    UNION ALL
    SELECT
        {{ automate_dv.hash("'OTHER_CLIENTS'", 'CUSTOMER_HK') }}, -- noqa
        "OTHER_CLIENTS" AS customer_bk
),

hub_site AS (
    SELECT
        site_hk,
        site_bk
    FROM {{ ref('rv_hub__site') }}
),

sat_forecast AS (
    SELECT
        demand_forecast_hk,
        start_datetime,
        end_datetime,
        date,
        end_date,
        forecast,
        event_forecast,
        total_confirmed_demand,
        record_source
    FROM {{ ref('bv_sat__demand_forecast') }}
),

sat_forecast_attributes AS (
    SELECT
        demand_forecast_hk,
        start_datetime,
        end_datetime,
        trend,
        use_aggregated_client_forecast
    FROM {{ ref('bv_sat__demand_forecast_article_attributes_bs') }}
),

joined AS (
    SELECT
        CONCAT(
            hub_fc.demand_forecast_bk,
            "||",
            sat.start_datetime,
            "||",
            COALESCE(sat.end_datetime, {{ var('end_date') }})
        ) AS pk_demand_forecast_code,
        hub_fc.demand_forecast_bk AS fk_demand_forecast_code,
        sat.date AS start_date,
        sat.end_date,
        sat_attr.trend,
        hub_article.article_bk AS fk_article_code,
        hub_site.site_bk AS fk_site_code,
        hub_customer.customer_bk AS fk_customer_code,
        CAST(sat.forecast AS integer) AS base_forecast,
        sat.event_forecast,
        CAST(sat.total_confirmed_demand AS integer) AS confirmed_extra,
        CAST(sat.forecast AS integer)
        + sat.event_forecast
        + CAST(sat.total_confirmed_demand AS integer) AS total_forecast,
        COALESCE(sat_attr.use_aggregated_client_forecast, false) AS aggregated_forecast_indicator,
        sat.start_datetime AS valid_from,
        sat.end_datetime AS valid_to,
        sat.record_source
    FROM hub_demand_forecast AS hub_fc
    LEFT JOIN
        lnk__demand_forecast__site_article AS lnk_df_sa
        ON hub_fc.demand_forecast_hk = lnk_df_sa.demand_forecast_hk
    LEFT JOIN hub_article ON lnk_df_sa.article_hk = hub_article.article_hk
    LEFT JOIN hub_site ON lnk_df_sa.site_hk = hub_site.site_hk
    LEFT JOIN lnk__demand_forecast__customer AS lnk_df_c ON hub_fc.demand_forecast_hk = lnk_df_c.demand_forecast_hk
    LEFT JOIN hub_customer ON lnk_df_c.customer_hk = hub_customer.customer_hk
    LEFT JOIN sat_forecast AS sat ON hub_fc.demand_forecast_hk = sat.demand_forecast_hk
    LEFT JOIN sat_forecast_attributes AS sat_attr
        ON
            hub_fc.demand_forecast_hk = sat_attr.demand_forecast_hk
            AND sat.start_datetime >= sat_attr.start_datetime
            AND sat.start_datetime < COALESCE(sat_attr.end_datetime, {{ var('end_date') }})
)

SELECT
    pk_demand_forecast_code,
    fk_demand_forecast_code,
    "MISSING" AS fk_promotion_code,
    fk_article_code,
    fk_site_code,
    fk_customer_code,
    start_date,
    end_date,
    trend,
    base_forecast,
    confirmed_extra,
    event_forecast,
    total_forecast,
    aggregated_forecast_indicator,
    "MISSING" AS seasonal_significance,
    "MISSING" AS seasonal_adjustment,
    "MISSING" AS seasonal_method,
    "MISSING" AS seasonal_years_to_use,
    "MISSING" AS seasonal_excluded_months,
    "MISSING" AS seasonal_warm_weather_buffer,
    "MISSING" AS seasonal_applied_pattern,
    valid_from,
    valid_to,
    record_source
FROM joined
