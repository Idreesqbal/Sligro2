{{
  config(
    partition_by={
      "field": "creation_date",
      "data_type": "date",
      "granularity": "month"
    }
  )
}}

WITH

bv__purchase_order AS (
    SELECT
        lvnrba,
        fojjba,
        fonrba,
        dckdba,
        stkdba,
        stkdba_description,
        iosoba_description,
        znkdba_description,
        twioba,
        lvdtba,
        iosoba,
        feusba,
        iodtba,
        zndtba,
        finrba,
        look_up_site_code,
        znkdba,
        purchase_order_hk,
        ionrba,
        tacoba,
        start_datetime,
        end_datetime,
        load_datetime
    FROM {{ ref('bv_bdv__purchase_order') }}
),

bv__purchase_order_extended AS (
    SELECT DISTINCT
        purchase_order_hk,
        fokdc1,
        ornrc5
    FROM {{ ref('bv_bdv__purchase_order_extended') }}
    WHERE end_datetime IS null
),

bv_hub__purchase_order AS (
    SELECT
        purchase_order_bk,
        purchase_order_hk
    FROM {{ ref('bv_hub__purchase_order') }}
),

lnk_purchase_order_supplier AS (
    SELECT
        purchase_order_hk,
        supplier_hk,
        purchase_order_supplier_hk
    FROM {{ ref('bv_lnk__purchase_order__supplier') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY purchase_order_hk ORDER BY load_datetime DESC) = 1
),

lnk_purchase_order_site AS (
    SELECT
        purchase_order_hk,
        site_hk,
        purchase_order_site_hk
    FROM {{ ref('bv_lnk__purchase_order__site') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY purchase_order_hk ORDER BY load_datetime DESC) = 1
),

hub_supplier AS (
    SELECT
        supplier_bk,
        supplier_hk
    FROM {{ ref('rv_hub__supplier') }}
),

hub_site AS (
    SELECT
        site_bk,
        site_hk
    FROM {{ ref('rv_hub__site') }}

),

final AS (
    SELECT
        bv_hub__purchase_order.purchase_order_bk AS fk_purchase_order_code,
        bv__purchase_order.ionrba AS purchase_order_code,
        hub_supplier.supplier_bk AS fk_supplier_code,
        CONCAT(
            bv__purchase_order_extended.fokdc1, '||',
            bv__purchase_order.fojjba, '||',
            bv__purchase_order.fonrba
        ) AS fk_promotion_code,
        bv__purchase_order.dckdba AS distribution_center,
        bv__purchase_order.stkdba AS purchase_order_status,
        bv__purchase_order.stkdba_description AS purchase_order_status_description,
        bv__purchase_order.iodtba AS creation_date,
        bv__purchase_order.zndtba AS ordered_date,
        bv__purchase_order.lvdtba AS requested_delivery_date,
        bv__purchase_order.twioba AS total_amount_excluding_vat,
        IF(
            hub_site.site_bk IS null,
            bv__purchase_order.look_up_site_code,
            hub_site.site_bk
        ) AS fk_ship_to_site_code,
        bv__purchase_order.fojjba AS folder_year,
        bv__purchase_order.fonrba AS folder_number,
        bv__purchase_order.feusba AS fk_requester_code,
        bv__purchase_order.iosoba AS purchase_order_type,
        bv__purchase_order.iosoba_description AS purchase_order_type_description,
        bv__purchase_order.znkdba AS communication_method,
        bv__purchase_order.tacoba AS total_amount_excise_duties,
        /*Using a macro now for the descriptions,
        waiting on the business to include it into sligro60_slgmutlib_slgfilelib.omstabpf*/
        bv__purchase_order.znkdba_description AS communication_method_description,
        bv__purchase_order.start_datetime AS valid_from,
        bv__purchase_order.end_datetime AS valid_to,
        CONCAT(
            bv_hub__purchase_order.purchase_order_bk,
            '||',
            bv__purchase_order.start_datetime,
            '||',
            COALESCE(bv__purchase_order.end_datetime, {{ var('end_date') }})
        ) AS pk_purchase_order_history,
        bv__purchase_order.load_datetime
    FROM bv__purchase_order
    LEFT JOIN bv_hub__purchase_order
        ON bv__purchase_order.purchase_order_hk = bv_hub__purchase_order.purchase_order_hk
    LEFT JOIN lnk_purchase_order_site
        ON bv_hub__purchase_order.purchase_order_hk = lnk_purchase_order_site.purchase_order_hk
    LEFT JOIN hub_site
        ON lnk_purchase_order_site.site_hk = hub_site.site_hk
    LEFT JOIN lnk_purchase_order_supplier
        ON bv_hub__purchase_order.purchase_order_hk = lnk_purchase_order_supplier.purchase_order_hk
    LEFT JOIN hub_supplier
        ON lnk_purchase_order_supplier.supplier_hk = hub_supplier.supplier_hk
    LEFT JOIN bv__purchase_order_extended
        ON bv_hub__purchase_order.purchase_order_hk = bv__purchase_order_extended.purchase_order_hk
    WHERE bv_hub__purchase_order.purchase_order_hk IS NOT null
)

/*"Missing" indicates that no source was found for the FK and links need to be created afterwards.
 "Unkown" indicates that the attribute is in the logical data model but it is not present in the source tables.*/
SELECT
    pk_purchase_order_history,
    fk_purchase_order_code,
    fk_supplier_code,
    fk_ship_to_site_code,
    fk_promotion_code,
    'Missing' AS fk_cost_centre_code,
    'Missing' AS fk_currency_code,
    fk_requester_code, -- This is always euro
    purchase_order_code,
    purchase_order_type,
    purchase_order_type_description,
    purchase_order_status,
    purchase_order_status_description,
    communication_method,
    communication_method_description,
    creation_date,
    ordered_date,
    requested_delivery_date,
    total_amount_excluding_vat,
    total_amount_excise_duties,
    'Unknown' AS vat_amount,
    'Unknown' AS total_amount,
    'Unknown' AS comments,
    distribution_center,
    load_datetime,
    valid_from,
    valid_to
FROM final
