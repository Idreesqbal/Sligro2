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
iodtba is the purchase order creation date.
This value can't be empty because we need it to create the new Purchase order BK.
Purchase order codes are recycled over time within the source system.
This was agreed with the business on 20/03/2025.
#}
WITH rv_sat__purchase_order AS (
    SELECT *
    FROM {{ ref('rv_sat__purchase_order') }}
    WHERE iodtba IS NOT null
),

intercompany_supplier AS (
    SELECT
        supplier_hk,
        intercompany_indicator
    FROM {{ ref('bv_sat__supplier_general_info') }}
    WHERE intercompany_indicator = true AND end_datetime IS null
),

{# Create new purchase order bk using business logic. #}
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
        stkdba_description,
        iosoba_description,
        znkdba_description,
        lvnrba AS supplier_bk,
        if(finrba = '0' OR finrba IS null, look_up_site_code, finrba) AS site_bk,
        concat(ionrba, '||', iodtba) AS purchase_order_bk
    FROM rv_sat__purchase_order
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
        stkdba_description,
        iosoba_description,
        znkdba_description,
        purchase_order_bk,
        upper(to_hex(sha256(supplier_bk))) AS supplier_hk,
        upper(to_hex(sha256(site_bk))) AS site_hk,
        upper(to_hex(sha256(purchase_order_bk || site_bk))) AS purchase_order_site_hk,
        upper(to_hex(sha256(purchase_order_bk || supplier_bk))) AS purchase_order_supplier_hk,
        upper(to_hex(sha256(purchase_order_bk))) AS purchase_order_hk
    FROM unique_key_creation
),

{# End date the purchase_order_hk based on the start_datetime.
This is needed so that purchase orders with the same ID but different creation
dates can be active at the same time and we don't loose history. #}
end_date_calculation AS (
    SELECT
        * EXCEPT (end_datetime),
        lag(start_datetime) OVER (
            PARTITION BY purchase_order_hk
            ORDER BY start_datetime DESC
        ) AS end_datetime
    FROM create_hashed_columns
),

{#
Purchase orders from Site to Site need to be removed because it belongs to Stock transfer order.
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
