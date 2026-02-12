{{ config(
    partition_by={
      "field": "load_datetime",
      "data_type": "timestamp",
      "granularity": "month"
    }
  )
}}

WITH hub_goods_received AS (
    SELECT
        goods_received_bk,
        goods_received_hk
    FROM {{ ref("bv_hub__goods_received") }}
),

hub_article AS (
    SELECT
        article_bk,
        article_hk
    FROM {{ ref("rv_hub__article_AS400") }}
),

hub_stock_location AS (
    SELECT
        stock_location_bk,
        stock_location_hk
    FROM {{ ref("rv_hub__stock_location") }}
),

hub_promotion AS (
    SELECT
        promotion_bk,
        promotion_hk
    FROM {{ ref("rv_hub__promotion") }}
),

hlnk_goods_received_article AS (
    SELECT
        goods_received_article_hk,
        goods_received_hk,
        article_hk
    FROM {{ ref("bv_hlnk__goods_received__article") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY goods_received_article_hk ORDER BY load_datetime DESC) = 1
),

hlnk_goods_received_article_promotion AS (
    SELECT
        goods_received_hk,
        article_hk,
        promotion_hk,
        promotion_article_goods_received_hk
    FROM {{ ref("bv_hlnk__goods_received__article__promotion") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promotion_article_goods_received_hk ORDER BY load_datetime DESC) = 1
),

hlnk_goods_received_article_stock_location AS (
    SELECT
        goods_received_hk,
        article_hk,
        stock_location_hk,
        goods_received_article_stock_location_hk
    FROM {{ ref("bv_hlnk__goods_received__article__stock_location") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY goods_received_article_stock_location_hk ORDER BY load_datetime DESC) = 1
),

goods_received_pallet AS (
    SELECT
        goods_received_hk,
        stock_location_hk,
        promotion_hk,
        article_hk,
        goods_received_article_hk,
        panrc1,
        load_datetime,
        start_datetime,
        end_datetime,
        goods_received_article_stock_location_hk,
        promotion_article_goods_received_hk
    FROM {{ ref("bv_bdv__goods_received_line_pallet") }}
    WHERE end_datetime IS null
),

final AS (
    SELECT DISTINCT
        hub_goods_received.goods_received_bk AS fk_goods_received_code,
        hub_stock_location.stock_location_bk AS fk_stock_location_code,
        hub_promotion.promotion_bk AS fk_promotion_code,
        hub_article.article_bk AS sligro_article_number,
        'Missing' AS fk_trade_item_code,
        CAST(goods_received_pallet.panrc1 AS string) AS pallet_number,
        goods_received_pallet.start_datetime AS valid_from,
        goods_received_pallet.end_datetime AS valid_to,
        goods_received_pallet.load_datetime
    FROM hub_goods_received
    INNER JOIN hlnk_goods_received_article AS lnk
        ON hub_goods_received.goods_received_hk = lnk.goods_received_hk
    INNER JOIN goods_received_pallet
        ON lnk.goods_received_article_hk = goods_received_pallet.goods_received_article_hk
    LEFT JOIN hub_article
        ON lnk.article_hk = hub_article.article_hk
    LEFT JOIN hlnk_goods_received_article_stock_location AS stock_location_lnk
        ON
            goods_received_pallet.goods_received_article_stock_location_hk
            = stock_location_lnk.goods_received_article_stock_location_hk
    LEFT JOIN hub_stock_location
        ON stock_location_lnk.stock_location_hk = hub_stock_location.stock_location_hk
    LEFT JOIN hlnk_goods_received_article_promotion AS promotion_lnk
        ON goods_received_pallet.promotion_article_goods_received_hk = promotion_lnk.promotion_article_goods_received_hk
    LEFT JOIN hub_promotion
        ON promotion_lnk.promotion_hk = hub_promotion.promotion_hk
    WHERE hub_goods_received.goods_received_bk IS NOT null
)

SELECT
    CONCAT(pallet_number, '||', fk_goods_received_code, '||', sligro_article_number) AS pk_goods_received_pallet_code,
    fk_goods_received_code,
    fk_stock_location_code,
    fk_promotion_code,
    fk_trade_item_code,
    sligro_article_number,
    pallet_number,
    load_datetime,
    valid_from,
    valid_to
FROM final
