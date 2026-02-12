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
        goods_received_hk,
        load_datetime
    FROM {{ ref("bv_hub__goods_received") }}
),

hub_purchase_order AS (
    SELECT
        purchase_order_bk,
        purchase_order_hk
    FROM {{ ref("bv_hub__purchase_order") }}
),

lnk_goods_received_purchase_order_site_supplier AS (
    SELECT
        goods_received_purchase_order_site_supplier_hk,
        goods_received_hk,
        purchase_order_hk,
        site_hk,
        supplier_hk,
        load_datetime
    FROM {{ ref("bv_lnk__goods_received__purchase_order__site__supplier") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY goods_received_hk ORDER BY load_datetime DESC) = 1
),

goods_received_header AS (
    SELECT
        ionrba,
        lvnrba,
        finrba,
        look_up_site_code,
        znkdba,
        stkdba,
        stkdba_description,
        znkdba_description,
        lvdtba,
        godtba,
        goods_received_hk,
        lvtdba,
        load_datetime,
        start_datetime,
        end_datetime
    FROM {{ ref("bv_bdv__goods_received") }}
    WHERE end_datetime IS null
),

first_record AS (
    SELECT
        lvdtba,
        goods_received_hk
    FROM {{ ref("bv_bdv__goods_received") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY goods_received_hk ORDER BY load_datetime ASC) = 1
),

date_count AS (
    SELECT
        goods_received_hk,
        COUNT(DISTINCT lvdtba) AS planned_date_change_count
    FROM {{ ref("bv_bdv__goods_received") }}
    GROUP BY goods_received_hk
),

hub_supplier AS (
    SELECT
        supplier_hk,
        supplier_bk
    FROM {{ ref("rv_hub__supplier") }}
),

hub_site AS (
    SELECT
        site_hk,
        site_bk
    FROM {{ ref("rv_hub__site") }}
),

final AS (
    SELECT
        hub_goods_received.goods_received_bk AS pk_goods_received_code,
        goods_received_header.ionrba AS goods_received_code,
        hub_purchase_order.purchase_order_bk AS fk_purchase_order_code,
        'Missing' AS fk_advanced_shipping_notification_code,
        hub_supplier.supplier_bk AS fk_supplier_code,
        hub_site.site_bk AS fk_site_code,
        goods_received_header.znkdba AS communication_method,
        goods_received_header.znkdba_description AS communication_method_description,
        goods_received_header.stkdba AS goods_received_delivery_status,
        goods_received_header.stkdba_description AS goods_received_delivery_status_description,
        goods_received_header.lvdtba AS current_planned_date,
        goods_received_header.godtba AS received_date,
        first_record.lvdtba AS original_planned_date,
        date_count.planned_date_change_count,
        goods_received_header.load_datetime,
        goods_received_header.start_datetime AS valid_from,
        goods_received_header.end_datetime AS valid_to
    FROM hub_goods_received
    LEFT JOIN lnk_goods_received_purchase_order_site_supplier AS lnk
        ON hub_goods_received.goods_received_hk = lnk.goods_received_hk
    INNER JOIN goods_received_header
        ON lnk.goods_received_hk = goods_received_header.goods_received_hk
    LEFT JOIN hub_supplier
        ON lnk.supplier_hk = hub_supplier.supplier_hk
    LEFT JOIN hub_site
        ON lnk.site_hk = hub_site.site_hk
    LEFT JOIN hub_purchase_order
        ON lnk.purchase_order_hk = hub_purchase_order.purchase_order_hk
    LEFT JOIN first_record
        ON lnk.goods_received_hk = first_record.goods_received_hk
    LEFT JOIN date_count
        ON hub_goods_received.goods_received_hk = date_count.goods_received_hk
    WHERE hub_goods_received.goods_received_bk IS NOT null
)

SELECT
    pk_goods_received_code,
    fk_purchase_order_code,
    fk_supplier_code,
    fk_advanced_shipping_notification_code,
    fk_site_code,
    goods_received_code,
    communication_method,
    communication_method_description,
    goods_received_delivery_status,
    goods_received_delivery_status_description,
    original_planned_date,
    current_planned_date,
    received_date,
    planned_date_change_count,
    load_datetime,
    valid_from,
    valid_to
FROM final
