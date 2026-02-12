{{
  config(
    partition_by={
      "field": "valid_from",
      "data_type": "date",
      "granularity": "month"
    }
  )
}}

WITH bv_line AS (
    SELECT
        purchase_order_line_bk,
        purchase_order_line_hk,
        arnrbe,
        ivaabe,
        bsaabe,
        inprbe,
        kopcbe,
        aiprbe,
        btkdbe,
        vpombe,
        irnrbe,
        acbdbe,
        start_datetime,
        end_datetime,
        load_datetime,
        btkdbe_description
    FROM {{ ref('bv_bdv__purchase_order_line') }}
),

bv_hub__purchase_order AS (
    SELECT * FROM {{ ref('bv_hub__purchase_order') }}
),

bv_lnk_purchase_order_article AS (
    SELECT * FROM {{ ref('bv_hlnk__purchase_order__article') }}
),

hub_article AS (
    SELECT * FROM {{ ref('rv_hub__article_AS400') }}
),

final AS (
    SELECT
        bv_line.purchase_order_line_bk AS fk_purchase_order_line_code,
        bv_hub__purchase_order.purchase_order_bk AS fk_purchase_order_code,
        CAST(bv_line.irnrbe AS BIGNUMERIC) AS purchase_order_line_number,
        hub_article.article_bk AS sligro_article_number,
        SAFE_MULTIPLY(bv_line.aiprbe, bv_line.bsaabe) AS line_total_discounted_amount_excluding_vat,
        SAFE_MULTIPLY(bv_line.inprbe, bv_line.bsaabe) AS line_total_amount_excluding_vat,
        ROUND(CAST(
            bv_line.inprbe + SAFE_MULTIPLY(bv_line.inprbe, {{ vat_rate('bv_line.btkdbe') }})
            AS BIGNUMERIC
        ), 2) AS article_unit_price_including_vat,
        bv_line.inprbe AS article_unit_price_excluding_vat,
        bv_line.kopcbe AS discount_amount,
        bv_line.aiprbe AS article_custom_unit_price_excluding_vat,
        bv_line.ivaabe AS quantity_uom,
        bv_line.bsaabe AS quantity_ordered,
        bv_line.btkdbe AS vat_rate,
        ROUND(bv_line.btkdbe_description, 2) AS vat_percentage,
        bv_line.vpombe AS unit_of_measure,
        bv_line.start_datetime AS valid_from,
        bv_line.end_datetime AS valid_to,
        /* Dividing by 100 because of data quality issues*/
        bv_line.acbdbe / 100 AS article_excise_duties,
        CONCAT(
            bv_line.purchase_order_line_bk,
            '||',
            bv_line.start_datetime,
            '||',
            COALESCE(bv_line.end_datetime, {{ var('end_date') }})
        ) AS pk_purchase_order_line_history,
        bv_line.load_datetime
    FROM bv_line
    LEFT JOIN bv_lnk_purchase_order_article
        ON bv_line.purchase_order_line_hk = bv_lnk_purchase_order_article.purchase_order_line_hk
    LEFT JOIN bv_hub__purchase_order
        ON bv_lnk_purchase_order_article.purchase_order_hk = bv_hub__purchase_order.purchase_order_hk
    LEFT JOIN hub_article
        ON bv_lnk_purchase_order_article.article_hk = hub_article.article_hk
    WHERE bv_hub__purchase_order.purchase_order_bk IS NOT null
)

/*"Missing" indicates that no source was found for the FK and links need to be created afterwards.
 "Unkown" indicates that the attribute is in the logical data model but it is not present in the source tables.*/
SELECT
    pk_purchase_order_line_history,
    fk_purchase_order_line_code,
    fk_purchase_order_code,
    'Missing' AS fk_trade_item_code,
    purchase_order_line_number,
    sligro_article_number,
    unit_of_measure,
    quantity_uom,
    quantity_ordered,
    article_unit_price_including_vat,
    article_unit_price_excluding_vat,
    article_excise_duties,
    discount_amount,
    article_custom_unit_price_excluding_vat,
    line_total_discounted_amount_excluding_vat,
    line_total_amount_excluding_vat,
    vat_rate,
    vat_percentage,
    'Unknown' AS comments,
    load_datetime,
    valid_from,
    valid_to
FROM final
