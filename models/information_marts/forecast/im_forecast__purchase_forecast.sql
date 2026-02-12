{{ config(
    partition_by={
      "field": "planned_order_placement_date",
      "data_type": "DATE"
    }
  )
}}

WITH hub_purchase_forecast AS (
    SELECT
        purchase_forecast_bk,
        purchase_forecast_hk
    FROM {{ ref('bv_hub__purchase_forecast') }}
),

hub_demand_forecast AS (
    SELECT
        demand_forecast_bk,
        demand_forecast_hk
    FROM {{ ref('bv_hub__demand_forecast') }}
),

hub_article AS (
    SELECT
        article_hk,
        article_bk
    FROM {{ ref('rv_hub__article_AS400') }}
),

hub_site AS (
    SELECT
        site_hk,
        site_bk
    FROM {{ ref('rv_hub__site') }}
),

lnk_purchase_forecast__site_article AS (
    SELECT
        purchase_forecast_hk,
        article_hk,
        site_hk
    FROM {{ ref('bv_lnk__purchase_forecast__site_article') }}
),

lnk__demand_forecast__purchase_forecast AS (
    SELECT
        demand_forecast_hk,
        purchase_forecast_hk
    FROM {{ ref('bv_lnk__demand_forecast__purchase_forecast') }}
),

sat_forecast AS (
    SELECT
        purchase_forecast,
        order_date,
        delivery_date,
        start_datetime,
        end_datetime,
        record_source,
        purchase_forecast_hk
    FROM {{ ref('bv_sat__purchase_forecast') }}
),

joined AS (
    SELECT
        CONCAT(
            hub_p_fc.purchase_forecast_bk,
            "||",
            hub_df.demand_forecast_bk,
            "||",
            sat.start_datetime,
            "||",
            COALESCE(sat.end_datetime, {{ var('end_date') }})
        ) AS pk_purchase_forecast_code,
        hub_p_fc.purchase_forecast_bk AS fk_purchase_forecast_code,
        hub_article.article_bk AS fk_article_code,
        hub_site.site_bk AS fk_site_code,
        hub_df.demand_forecast_bk AS fk_demand_forecast_code,
        CAST(sat.purchase_forecast AS integer) AS planned_purchase_quantity,
        sat.order_date AS planned_order_placement_date,
        sat.delivery_date AS expected_delivery_date,
        sat.start_datetime AS valid_from,
        sat.end_datetime AS valid_to,
        sat.record_source
    FROM hub_purchase_forecast AS hub_p_fc
    LEFT JOIN
        lnk_purchase_forecast__site_article AS lnk_pf_sa
        ON hub_p_fc.purchase_forecast_hk = lnk_pf_sa.purchase_forecast_hk
    LEFT JOIN hub_article ON lnk_pf_sa.article_hk = hub_article.article_hk
    LEFT JOIN hub_site ON lnk_pf_sa.site_hk = hub_site.site_hk
    LEFT JOIN
        lnk__demand_forecast__purchase_forecast AS lnk_fc_pp
        ON hub_p_fc.purchase_forecast_hk = lnk_fc_pp.purchase_forecast_hk
    LEFT JOIN hub_demand_forecast AS hub_df ON lnk_fc_pp.demand_forecast_hk = hub_df.demand_forecast_hk
    LEFT JOIN sat_forecast AS sat ON hub_p_fc.purchase_forecast_hk = sat.purchase_forecast_hk
)

SELECT
    pk_purchase_forecast_code,
    fk_purchase_forecast_code,
    fk_article_code,
    fk_site_code,
    fk_demand_forecast_code,
    "MISSING" AS fk_supply_information_code,
    "MISSING" AS fk_stock_reorder_code,
    "MISSING" AS planned_purchase_date,
    planned_purchase_quantity,
    planned_order_placement_date,
    expected_delivery_date,
    valid_from,
    valid_to,
    record_source
FROM joined
