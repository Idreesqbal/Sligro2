WITH rv_sat_bs AS (
    SELECT *
    FROM {{ ref('rv_sat__forecast_bs') }}
),

rv_sat_cdc AS (
    SELECT *
    FROM {{ ref('rv_sat__forecast_cdc') }}
),

cdc_updates AS (
    SELECT
        * REPLACE (
            CASE site_bk
                WHEN 'D' THEN '{{ var('cdc_sitecode') }}'
                ELSE ERROR(CONCAT('No mapping defined for store_number of forecast cdc: ', site_bk))
            END AS site_bk
        )
    FROM rv_sat_cdc
),

bs_updates AS (
    SELECT
        * REPLACE (
            CASE
                WHEN customer_bk = '00000S4' THEN 'OTHER_CLIENTS'
                ELSE CAST(CAST(customer_bk AS INTEGER) AS STRING)
                --bs customer adapt from placeholder to "other clients"
                --bs customer adapt from string value to real integer and then string value to remove leading zeros
            END AS customer_bk
        )
    FROM rv_sat_bs
),

union_all AS (
    SELECT * FROM cdc_updates
    UNION ALL
    SELECT * FROM bs_updates
),

general_updates AS (
    SELECT
        union_all.*,
        IF(union_all.day_number = 0, union_all.date + 7, union_all.date) AS end_date
    FROM union_all
),

forecast_updates_new_bk AS (
    SELECT
        *,
        CONCAT(
            article_bk,
            '||',
            COALESCE(customer_bk, ''),
            '||',
            site_bk,
            '||',
            date,
            '||',
            end_date
        ) AS demand_forecast_bk,
        IF(
            purchase_forecast > 0 AND customer_bk IS null,
            CONCAT(
                article_bk,
                '||',
                site_bk,
                '||',
                order_date
            ),
            null
        ) AS purchase_forecast_bk
    FROM general_updates
),

combined AS (
    SELECT
        *,
        CONCAT(demand_forecast_bk, '||', COALESCE(purchase_forecast_bk, '')) AS demand_forecast_purchase_forecast_bk
    FROM
        forecast_updates_new_bk
)

SELECT
    *,
    {{ automate_dv.hash('CUSTOMER_BK', 'CUSTOMER_HK') }}, -- noqa
    {{ automate_dv.hash('SITE_BK', 'SITE_HK') }}, -- noqa
    {{ automate_dv.hash('ARTICLE_BK', 'ARTICLE_HK') }}, -- noqa
    {{ automate_dv.hash('DEMAND_FORECAST_BK', 'DEMAND_FORECAST_HK') }}, -- noqa
    {{ automate_dv.hash('PURCHASE_FORECAST_BK', 'PURCHASE_FORECAST_HK') }}, -- noqa
    {{ automate_dv.hash('DEMAND_FORECAST_PURCHASE_FORECAST_BK', 'DEMAND_FORECAST_PURCHASE_FORECAST_HK') }} -- noqa
FROM combined
