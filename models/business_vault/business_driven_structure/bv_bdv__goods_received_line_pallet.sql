{{
  config(
    partition_by={
      "field": "LOAD_DATETIME",
      "data_type": "timestamp",
      "granularity": "month"
    }
  )
}}

WITH header AS (
    SELECT
        load_datetime,
        start_datetime,
        end_datetime,
        ionrba,
        purchase_order_bk,
        purchase_order_hk,
        goods_received_bk,
        goods_received_hk
    FROM {{ ref('bv_bdv__goods_received') }}
),

{# We need to extract the different stock location components from the loknc1 column.
This was done following the definition from Stock location for CDC on Confluence. #}
line_pallet AS (
    SELECT
        *,
        SUBSTR(loknc1, 1, 3) AS dc_row,
        SUBSTR(loknc1, 4, 2) AS dc_house,
        SUBSTR(loknc1, 6, 1) AS dc_height,
        SUBSTR(loknc1, 7, 2) AS dc_place
    FROM {{ ref('rv_sat__goods_received_line_pallet') }}
),

{# Create new goods reiceved pallet bk using business logic.
Codes are recycled in the source system.
We need to join with the header because we don't have the received date in the pallet information.
We use QUALIFY to remove duplicates. #}
unique_key_creation AS (
    SELECT
        line_pallet.arnrc1,
        line_pallet.cydxc1,
        line_pallet.dckdc1,
        line_pallet.eidec1,
        line_pallet.fanrc1,
        line_pallet.finrc1,
        line_pallet.foejc1,
        line_pallet.fokdc1,
        line_pallet.fonrc1,
        line_pallet.godec1,
        line_pallet.godfc1,
        line_pallet.gotdc1,
        line_pallet.gousc1,
        line_pallet.ionrc1,
        line_pallet.klnrc1,
        line_pallet.loknc1,
        line_pallet.opdec1,
        line_pallet.optdc1,
        line_pallet.panrc1,
        line_pallet.plaac3,
        line_pallet.prtyc1,
        line_pallet.rtdec1,
        line_pallet.s021c1,
        line_pallet.thdec1,
        line_pallet.timxc1,
        line_pallet.update_ident,
        line_pallet.userc1,
        line_pallet.usrxc1,
        line_pallet.vedec2,
        line_pallet.vetdc2,
        line_pallet.veusc1,
        line_pallet.wnnrc1,
        line_pallet.wyejc1,
        line_pallet.end_datetime,
        line_pallet.start_datetime,
        line_pallet.record_source,
        line_pallet.load_datetime,
        header.purchase_order_bk,
        header.goods_received_bk,
        line_pallet.arnrc1 AS article_bk,
        header.purchase_order_hk,
        header.goods_received_hk,
        CONCAT(
            {{ var('cdc_sitecode') }},
            "||",
            line_pallet.dckdc1,
            "||",
            line_pallet.dc_row,
            "||",
            line_pallet.dc_house,
            "||",
            line_pallet.dc_height,
            "||",
            line_pallet.dc_place
        ) AS stock_location_bk,
        CONCAT(line_pallet.fokdc1, "||", line_pallet.foejc1, "||", line_pallet.fonrc1) AS promotion_bk,
        CONCAT(header.goods_received_bk, "||", line_pallet.arnrc1) AS goods_received_article_bk
    FROM line_pallet
    INNER JOIN header
        ON
            line_pallet.ionrc1 = header.ionrba
            AND CAST(line_pallet.start_datetime AS date) BETWEEN
            CAST(header.start_datetime AS date) AND COALESCE(CAST(header.end_datetime AS date), "9999-12-31")
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY header.goods_received_bk, line_pallet.arnrc1, line_pallet.start_datetime, line_pallet.panrc1
        ORDER BY line_pallet.start_datetime ASC, line_pallet.load_datetime DESC
    ) = 1
),

{# Hashing the BKs #}
apply_hashing AS (
    SELECT
        *,
        UPPER(TO_HEX(SHA256(article_bk))) AS article_hk,
        UPPER(TO_HEX(SHA256(purchase_order_bk || article_bk)))
            AS purchase_order_article_hk,
        UPPER(TO_HEX(SHA256(goods_received_bk || article_bk)))
            AS goods_received_article_hk,
        UPPER(TO_HEX(SHA256(stock_location_bk))) AS stock_location_hk,
        UPPER(TO_HEX(SHA256(goods_received_bk || article_bk || promotion_bk)))
            AS promotion_article_goods_received_hk,
        UPPER(TO_HEX(SHA256(promotion_bk))) AS promotion_hk,
        UPPER(TO_HEX(SHA256(goods_received_bk || article_bk || stock_location_bk)))
            AS goods_received_article_stock_location_hk,
        UPPER(TO_HEX(SHA256(goods_received_bk || article_bk || panrc1)))
            AS goods_received_article_pallet_hk
    FROM unique_key_creation
),

{# End date the goods_received_article_pallet_hk based on the start_datetime.
This is needed so that pallets with the same ID but different received
dates can be active at the same time and we don't loose history. #}
end_date_calculation AS (
    SELECT
        * EXCEPT (end_datetime),
        LAG(start_datetime) OVER (
            PARTITION BY goods_received_article_pallet_hk
            ORDER BY start_datetime DESC
        ) AS end_datetime
    FROM apply_hashing
)

SELECT * FROM end_date_calculation
