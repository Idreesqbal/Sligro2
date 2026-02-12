{{
  config(
    partition_by={
      "field": "LOAD_DATETIME",
      "data_type": "timestamp",
      "granularity": "month"
    }
  )
}}

{#
iodtba is the purchase order creation date and godtba is the goods received date.
Those values can't be empty because we need them to create the new Purchase order BK and Goods received BK.
Purchase order/Goods received codes are recycled over time within the source system.
This was agreed with the business on 20/03/2025.
#}
WITH rv_sat__goods_received AS (
    SELECT *
    FROM {{ ref('rv_sat__goods_received_header') }}
    WHERE iodtba IS NOT null AND godtba IS NOT null
),

intercompany_supplier AS (
    SELECT
        supplier_hk,
        intercompany_indicator
    FROM {{ ref('bv_sat__supplier_general_info') }}
    WHERE intercompany_indicator = true AND end_datetime IS null
),

{# Create new goods received and purchase order bks using business logic. #}
unique_key_creation AS (
    SELECT
        ahalba,
        argrba,
        bfdtba,
        btanba,
        cbdtba,
        codtba,
        dckdba,
        dockba,
        fedtba,
        fekdba,
        feusba,
        ffdtba,
        fgdtba,
        fiifba,
        finrba,
        fkdtba,
        fojjba,
        fonrba,
        gbdtba,
        godtba,
        gousba,
        hkdtba,
        hkusba,
        i1dtba,
        i2dtba,
        iodtba,
        ionrba,
        iosoba,
        iousba,
        ivdtba,
        jfdtba,
        jgdtba,
        jvdtba,
        kkdtba,
        lvdtba,
        lvnrba,
        lvtdba,
        ogdtba,
        orioba,
        podtba,
        srdtba,
        stkdba,
        tacoba,
        temoba,
        tikoba,
        toifba,
        toioba,
        twioba,
        tzboba,
        vfdtba,
        vvdtba,
        vvusba,
        zndtba,
        znkdba,
        look_up_site_code,
        load_datetime,
        start_datetime,
        end_datetime,
        record_source,
        lvnrba AS supplier_bk,
        stkdba_description,
        iosoba_description,
        znkdba_description,
        if(finrba = '0' OR finrba IS null, look_up_site_code, finrba) AS site_bk,
        concat(ionrba, '||', iodtba) AS purchase_order_bk,
        concat(ionrba, '||', godtba) AS goods_received_bk
    FROM rv_sat__goods_received
),

{# Hashing the BKs #}
create_hashed_columns AS (
    SELECT
        ahalba,
        argrba,
        bfdtba,
        btanba,
        cbdtba,
        codtba,
        dckdba,
        dockba,
        fedtba,
        fekdba,
        feusba,
        ffdtba,
        fgdtba,
        fiifba,
        finrba,
        fkdtba,
        fojjba,
        fonrba,
        gbdtba,
        godtba,
        gousba,
        hkdtba,
        hkusba,
        i1dtba,
        i2dtba,
        iodtba,
        ionrba,
        iosoba,
        iousba,
        ivdtba,
        jfdtba,
        jgdtba,
        jvdtba,
        kkdtba,
        lvdtba,
        lvnrba,
        lvtdba,
        ogdtba,
        orioba,
        podtba,
        srdtba,
        stkdba,
        tacoba,
        temoba,
        tikoba,
        toifba,
        toioba,
        twioba,
        tzboba,
        vfdtba,
        vvdtba,
        vvusba,
        zndtba,
        znkdba,
        start_datetime,
        look_up_site_code,
        load_datetime,
        end_datetime,
        record_source,
        purchase_order_bk,
        goods_received_bk,
        stkdba_description,
        iosoba_description,
        znkdba_description,
        upper(to_hex(sha256(supplier_bk))) AS supplier_hk,
        upper(to_hex(sha256(site_bk))) AS site_hk,
        upper(to_hex(sha256(purchase_order_bk || site_bk))) AS purchase_order_site_hk,
        upper(to_hex(sha256(purchase_order_bk || supplier_bk))) AS purchase_order_supplier_hk,
        upper(to_hex(sha256(purchase_order_bk))) AS purchase_order_hk,
        upper(to_hex(sha256(goods_received_bk))) AS goods_received_hk,
        upper(to_hex(sha256(goods_received_bk || purchase_order_bk || site_bk || supplier_bk)))
            AS goods_received_purchase_order_site_supplier_hk
    FROM unique_key_creation
),

{# End date the goods_received_hk based on the start_datetime.
This is needed so that goods received with the same ID but different received
dates can be active at the same time and we don't loose history. #}
end_date_calculation AS (
    SELECT
        * EXCEPT (end_datetime),
        lag(start_datetime) OVER (
            PARTITION BY goods_received_hk
            ORDER BY start_datetime DESC
        ) AS end_datetime
    FROM create_hashed_columns
),

{#
Goods received from Site to Site needs to be removed because it belongs to Stock transfer order.
Agreed with the Business on 12/02/2025.
#}
remove_stock_transfer_order AS (
    SELECT end_date_calculation.*
    FROM end_date_calculation
    LEFT JOIN intercompany_supplier ON end_date_calculation.supplier_hk = intercompany_supplier.supplier_hk
    WHERE intercompany_supplier.intercompany_indicator IS null
),

{#
There are data quality issues in a small amount of rows where Site codes are empty.
Those can be removed in agreement with Business on 23/04/2025.
#}
remove_empty_site_code AS (
    SELECT *
    FROM remove_stock_transfer_order
    WHERE site_hk IS NOT null
)

SELECT *
FROM remove_empty_site_code
