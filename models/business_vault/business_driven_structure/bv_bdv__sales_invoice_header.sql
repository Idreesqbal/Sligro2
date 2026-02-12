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

WITH hub_salesinvoice AS (
    SELECT
        sales_invoice_hk,
        sales_invoice_bk
    FROM {{ ref("rv_hub__sales_invoice") }}
),

salesinvoice AS (
    SELECT
        hub_salesinvoice.sales_invoice_hk,
        hub_salesinvoice.sales_invoice_bk,
        sat_salesinvoice.fanrc1,
        sat_salesinvoice.fktpc1,
        sat_salesinvoice.fkdec1,
        sat_salesinvoice.fktdc1,
        sat_salesinvoice.finrc1,
        sat_salesinvoice.dvnmc1,
        sat_salesinvoice.slorc1,
        sat_salesinvoice.srorc1,
        sat_salesinvoice.klnrc9,
        sat_salesinvoice.fkrsc1,
        sat_salesinvoice.betmc1,
        sat_salesinvoice.rpscc1,
        sat_salesinvoice.fkexc1,
        sat_salesinvoice.invoice_type_desc,
        sat_salesinvoice.currency_code,
        sat_salesinvoice.start_datetime,
        sat_salesinvoice.end_datetime,
        sat_salesinvoice.load_datetime
    FROM {{ ref("rv_sat__sales_invoice") }} AS sat_salesinvoice
    INNER JOIN hub_salesinvoice ON sat_salesinvoice.sales_invoice_hk = hub_salesinvoice.sales_invoice_hk
    WHERE
        sat_salesinvoice.end_datetime IS null
        --Business rule for excluding expired invoices from the overview
        --srorc1 = order type, and expired invoices have a value of 99
        AND sat_salesinvoice.srorc1 <> 99

),

sat_invoicedelivery AS (
    SELECT
        sales_invoice_hk,
        fknlc1,
        pgmtc1,
        program_desc
    FROM {{ ref("rv_masat__sales_invoice_delivery") }}
    WHERE end_datetime IS null
    --Exclusion of the batch invoice program codes.
    --Not required for the sales invoice overview and causes duplications in lines.
    AND pgmtc1 <> 'VFA010'
),

final AS (
    SELECT
        salesinvoice.sales_invoice_hk,
        salesinvoice.sales_invoice_bk,
        IF(salesinvoice.fkexc1 = 'J', true, false) AS derived_invoice_excl_vat_indicator,
        salesinvoice.fanrc1,
        salesinvoice.fktpc1,
        salesinvoice.fkdec1,
        salesinvoice.fktdc1,
        salesinvoice.finrc1,
        salesinvoice.dvnmc1,
        salesinvoice.slorc1,
        salesinvoice.srorc1,
        salesinvoice.klnrc9,
        salesinvoice.fkrsc1,
        salesinvoice.betmc1,
        salesinvoice.rpscc1,
        salesinvoice.fkexc1,
        salesinvoice.invoice_type_desc,
        salesinvoice.currency_code,
        salesinvoice.start_datetime,
        salesinvoice.end_datetime,
        salesinvoice.load_datetime,
        sat_invoicedelivery.fknlc1,
        sat_invoicedelivery.pgmtc1,
        sat_invoicedelivery.program_desc
    FROM salesinvoice
    LEFT JOIN sat_invoicedelivery ON salesinvoice.sales_invoice_hk = sat_invoicedelivery.sales_invoice_hk

)

SELECT *
FROM final

{% if is_incremental() %}
    WHERE
        load_datetime > (SELECT MAX(load_datetime) AS max_ldts FROM {{ this }})
        OR end_datetime > (SELECT MAX(end_datetime) AS max_ldts FROM {{ this }})
{% endif %}
