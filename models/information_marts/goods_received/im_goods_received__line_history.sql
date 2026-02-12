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

hlnk_goods_received_article AS (
    SELECT
        goods_received_article_hk,
        goods_received_hk,
        article_hk,
        load_datetime
    FROM {{ ref("bv_hlnk__goods_received__article") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY goods_received_article_hk ORDER BY load_datetime DESC) = 1
),

goods_received_line AS (
    SELECT
        irnrbe,
        ovaabe,
        ovkgbe,
        dcaabe,
        goods_received_line_hk,
        goods_received_article_hk,
        ionrbe,
        godtbe,
        goods_received_line_bk,
        start_datetime,
        end_datetime,
        load_datetime
    FROM {{ ref("bv_bdv__goods_received_line") }}
),

final AS (
    SELECT DISTINCT
        CONCAT(
            goods_received_line.goods_received_line_bk,
            "||",
            goods_received_line.start_datetime,
            "||",
            COALESCE(goods_received_line.end_datetime, {{ var('end_date') }})
        )
            AS pk_goods_received_line_code_history,
        goods_received_line.goods_received_line_bk AS fk_goods_received_line_code,
        hub_goods_received.goods_received_bk AS fk_goods_received_code,
        goods_received_line.ionrbe AS goods_received_code,
        CAST(goods_received_line.irnrbe AS BIGNUMERIC) AS goods_received_line_number,
        hub_article.article_bk AS sligro_article_number,
        "Missing" AS fk_trade_item_code,
        goods_received_line.ovaabe AS received_colli_quantity,
        goods_received_line.ovkgbe AS received_weight_kg,
        goods_received_line.goods_received_article_hk,
        IF(
            goods_received_line.dcaabe = 2, goods_received_line.ovkgbe,
            goods_received_line.ovaabe
        ) AS received_amount,
        goods_received_line.start_datetime AS valid_from,
        goods_received_line.end_datetime AS valid_to,
        goods_received_line.load_datetime
    FROM hub_goods_received
    INNER JOIN hlnk_goods_received_article AS lnk
        ON hub_goods_received.goods_received_hk = lnk.goods_received_hk
    INNER JOIN goods_received_line
        ON lnk.goods_received_article_hk = goods_received_line.goods_received_article_hk
    LEFT JOIN hub_article
        ON lnk.article_hk = hub_article.article_hk
    WHERE hub_goods_received.goods_received_bk IS NOT null
)

SELECT
    pk_goods_received_line_code_history,
    fk_goods_received_line_code,
    fk_goods_received_code,
    fk_trade_item_code,
    goods_received_code,
    goods_received_line_number,
    sligro_article_number,
    received_colli_quantity,
    received_amount,
    received_weight_kg,
    load_datetime,
    valid_from,
    valid_to
FROM final
