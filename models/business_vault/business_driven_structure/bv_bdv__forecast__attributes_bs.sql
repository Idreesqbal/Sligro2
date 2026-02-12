WITH bv_bdv__forecast AS (
    SELECT DISTINCT
        forecast_hk,
        demand_forecast_hk,
        demand_forecast_bk,
        purchase_forecast_hk,
        purchase_forecast_bk
    FROM {{ ref('bv_bdv__forecast__forecast') }}
)

SELECT
    rv_sat.* REPLACE (
        -- is the reverse in the source model, so we need to negate it here
        NOT rv_sat.is_stock_controlled AS is_stock_controlled
    ),
    bv_bdv__forecast.demand_forecast_hk,
    bv_bdv__forecast.demand_forecast_bk,
    bv_bdv__forecast.purchase_forecast_hk,
    bv_bdv__forecast.purchase_forecast_bk
FROM
    {{ ref('rv_sat__forecast_article_attributes_bs') }} AS rv_sat
INNER JOIN
    bv_bdv__forecast
    -- only include hk that are in forecast and article_attributes as it
    -- is possible that there are hks in article_attributes that are not in the forecast
    -- this means that business does not make a forecast for all their articles
    ON rv_sat.forecast_hk = bv_bdv__forecast.forecast_hk
