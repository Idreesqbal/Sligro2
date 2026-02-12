{{
  config(
    materialized='incremental',
    partition_by={
      "field": "LOAD_DATETIME",
      "data_type": "timestamp",
      "granularity": "month"
    }
  )
}}

--Collect all the facts and HK's from the sales invoice line link
WITH lnk_salesinvoiceline AS (
    SELECT
        sales_invoice_lines_transactions_hk,
        sales_invoice_hk,
        article_hk,
        promotion_hk,
        price_hk,
        site_hk,
        zbbxc1,
        brpxc1,
        fkaac1,
        koprc1,
        kopcc1,
        inprc1,
        zbprc1,
        emprc1
    FROM {{ ref("rv_tlnk__sales_invoice_lines_transactions") }}
),

--Collect all the attributes of the sales invoice lines
sat_salesinvoiceline AS (
    SELECT
        lnk.sales_invoice_lines_transactions_hk,
        lnk.sales_invoice_hk,
        lnk.article_hk,
        lnk.promotion_hk,
        lnk.price_hk,
        lnk.site_hk,
        sat.arnrc1,
        sat.btkdc1,
        sat.fanrc1,
        sat.finrc1,
        sat.fkdec1,
        sat.foejc1,
        sat.okkdc1,
        sat.fonrc1,
        sat.prnrc1,
        sat.volgc1,
        sat.psnrc1,
        sat.spnrc1,
        sat.klnuc1,
        lnk.brpxc1,
        lnk.koprc1,
        lnk.kopcc1,
        lnk.inprc1,
        lnk.zbbxc1,
        lnk.zbprc1,
        lnk.emprc1,
        sat.start_datetime,
        sat.end_datetime,
        sat.load_datetime,
        --Indicator if the amount is really a amount or weight.
        --If weight then it should be divided by 100.
        IF(sat.dcaac1 = 2, (lnk.fkaac1 / 100), lnk.fkaac1) AS fkaac1,
        --zbbxc1 = Berekende prijs , spnrc1 = Soort prijs
        IF(lnk.zbbxc1 = 0 AND sat.spnrc1 = 99, 'Heineken', 'SFG') AS derived_data_router_code
    FROM {{ ref("rv_sat__sales_invoice_line") }} AS sat
    INNER JOIN
        lnk_salesinvoiceline AS lnk
        ON sat.sales_invoice_lines_transactions_hk = lnk.sales_invoice_lines_transactions_hk
    WHERE sat.end_datetime IS null
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY lnk.sales_invoice_lines_transactions_hk, sat.end_datetime
        ORDER BY sat.end_datetime ASC, sat.load_datetime DESC
    ) = 1
),

--collect the sales invoice BK from business vault sales invoice header
salesinvoice_header AS (
    SELECT
        salesinvoice_header.sales_invoice_hk,
        salesinvoice_header.sales_invoice_bk,
        salesinvoice_header.fkexc1,
        --TDGP-1942: added to calculate the derived_transaction_type in the "final" cte below
        salesinvoice_header.rpscc1,
        --TDGP-1942: added to calculate the derived_transaction_type in the "final" cte below
        salesinvoice_header.slorc1
    FROM {{ ref("bv_bdv__sales_invoice_header") }} AS salesinvoice_header
),

--collect the country of a site so that it can be used in the connection to vat_amount
site_country AS (
    SELECT
        site_hk,
        {{ headquarter_to_country('FINRC4') }} AS country
    FROM {{ ref("rv_sat__site_details") }}
    WHERE end_datetime IS null
),

--collect the btw_percentage to calculate the vat_amount on the sales invoice lines
vat_amount AS (
    SELECT
        btw_bk,
        geldig_vanaf,
        COALESCE(geldig_tm, CAST({{ var('end_date') }} AS DATE)) AS geldig_tm,
        btw_percentage
    FROM {{ ref("ref__vat_amount") }}
),

--collect the vat description
vat_description AS (
    SELECT
        btw_bk,
        btw_oms
    FROM {{ ref("ref__vat_description") }}
),

final AS (
    SELECT
        salesinvoice_header.sales_invoice_bk,
        sat_salesinvoiceline.article_hk,
        sat_salesinvoiceline.promotion_hk,
        sat_salesinvoiceline.site_hk,
        sat_salesinvoiceline.prnrc1,
        --end test data columns
        sat_salesinvoiceline.psnrc1,
        sat_salesinvoiceline.fkdec1,
        sat_salesinvoiceline.arnrc1,
        --sat_salesinvoiceline.data_router_code, --TDGP-1942: phased out, replaced by derived_transaction_type
        sat_salesinvoiceline.zbbxc1,
        sat_salesinvoiceline.brpxc1,
        sat_salesinvoiceline.fkaac1,
        sat_salesinvoiceline.koprc1,
        sat_salesinvoiceline.zbprc1,
        sat_salesinvoiceline.kopcc1,
        sat_salesinvoiceline.spnrc1,
        sat_salesinvoiceline.btkdc1,
        sat_salesinvoiceline.klnuc1,
        salesinvoice_header.fkexc1,
        --TDGP-1942: Replaces data router_code logic and is renamed to derived_transaction_type
        CASE
            WHEN salesinvoice_header.rpscc1 = 9 THEN 'Crossdock HNK' --slowmovers/HNK settlements
            WHEN salesinvoice_header.slorc1 = 'IC' THEN 'Intercompany'
            ELSE sat_salesinvoiceline.derived_data_router_code
        END AS derived_transaction_type,
        ROUND(sat_salesinvoiceline.zbbxc1 * sat_salesinvoiceline.fkaac1, 2) AS net_amount,
        ROUND(sat_salesinvoiceline.koprc1 * sat_salesinvoiceline.fkaac1, 2) AS cost_value,
        ROUND(sat_salesinvoiceline.brpxc1 * sat_salesinvoiceline.fkaac1, 2) AS gross_value,
        ROUND(sat_salesinvoiceline.zbprc1 * sat_salesinvoiceline.fkaac1, 2) AS zb_value,
        -- First calculate discount per unit where round takes place over the whole number.
        ROUND(
            IF(
                sat_salesinvoiceline.kopcc1 = 0,
                0,
                sat_salesinvoiceline.brpxc1 * (sat_salesinvoiceline.kopcc1 / 100)
            ), 2
        ) AS discount_per_unit,
        /*
         First round up to 2 decimal the discount per unit.
        Then round up the full equation when multiplied with the quantity
        */
        ROUND(
            ROUND(
                IF(
                    sat_salesinvoiceline.kopcc1 = 0,
                    0,
                    sat_salesinvoiceline.brpxc1 * (sat_salesinvoiceline.kopcc1 / 100)
                ), 2
            ) * sat_salesinvoiceline.fkaac1, 2
        ) AS discount_value,
        -- Create the unique BK for a sales invoice line
        CONCAT(
            sat_salesinvoiceline.fanrc1,
            '||',
            sat_salesinvoiceline.finrc1,
            '||',
            FORMAT_DATE('%Y%m%d', sat_salesinvoiceline.fkdec1),
            '||',
            sat_salesinvoiceline.volgc1,
            '||',
            sat_salesinvoiceline.arnrc1,
            '||',
            sat_salesinvoiceline.psnrc1
        ) AS sales_invoice_line_code,
        --Moved vat reference table joins and logic to the business vault
        IF(
            salesinvoice_header.fkexc1 = 'J',
            0, ROUND((sat_salesinvoiceline.zbbxc1 * sat_salesinvoiceline.fkaac1) * (vat_amount.btw_percentage / 100), 2)
        )
            AS derived_vat_amount, --Calculation of the vat amount of the sales invoice line based on the net_amount.
        vat_amount.btw_percentage,
        --TDGP-1898: add the vat description
        vat_description.btw_oms,
        sat_salesinvoiceline.start_datetime,
        sat_salesinvoiceline.end_datetime,
        sat_salesinvoiceline.load_datetime
    FROM sat_salesinvoiceline
    INNER JOIN salesinvoice_header ON sat_salesinvoiceline.sales_invoice_hk = salesinvoice_header.sales_invoice_hk
    LEFT JOIN site_country ON sat_salesinvoiceline.site_hk = site_country.site_hk
    LEFT JOIN vat_amount
        ON
            CONCAT(sat_salesinvoiceline.btkdc1, '||', site_country.country) = vat_amount.btw_bk
            AND sat_salesinvoiceline.fkdec1 BETWEEN vat_amount.geldig_vanaf AND vat_amount.geldig_tm
    LEFT JOIN vat_description
        ON CONCAT(sat_salesinvoiceline.btkdc1, '||', site_country.country) = vat_description.btw_bk
)

SELECT
    sales_invoice_line_code,
    sales_invoice_bk,
    article_hk,
    promotion_hk,
    site_hk,
    arnrc1,
    prnrc1,
    psnrc1,
    fkdec1,
    --data_router_code, --TDGP-1942: phased out, replaced by derived_transaction_type
    derived_transaction_type, --TDGP-1942: added to add the transactiontype in the IM
    zbbxc1,
    brpxc1,
    fkaac1,
    koprc1,
    zbprc1,
    spnrc1,
    kopcc1,
    btkdc1,
    klnuc1,
    fkexc1,
    derived_vat_amount, --Moved vat reference table logic to the business vault
    btw_percentage, --Moved vat reference table logic to the business vault
    btw_oms, --Moved vat reference table logic to the business vault
    net_amount,
    cost_value,
    gross_value,
    zb_value,
    discount_per_unit,
    discount_value,
    start_datetime,
    end_datetime,
    load_datetime
FROM final



{% if is_incremental() %}
    WHERE
        load_datetime > (SELECT MAX(load_datetime) AS max_ldts FROM {{ this }})
        OR end_datetime > (SELECT MAX(end_datetime) AS max_ldts FROM {{ this }})
{% endif %}
