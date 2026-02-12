{{ config(
    unique_key = ['pk_sales_invoice_code', 'program_code']
    , partition_by={
        'field': 'date_generated',
        'data_type': 'date'}
) }}

--Collect all sales invoice data from the hub_sales_invoice, sat_sales_invoice and sales_invoice_delivery
--All data comes from the business vault sales invoice header
WITH

bv_salesinvoice AS (
    SELECT *
    FROM {{ ref("bv_bdv__sales_invoice_header") }}
),

--Collect fk_site
hub_site AS (
    SELECT
        site_hk,
        site_bk
    FROM {{ ref("rv_hub__site") }}
),

final AS (
    SELECT
        bv_salesinvoice.sales_invoice_bk AS sales_invoice_code,
        'Missing' AS legal_entity_code,
        hub_site.site_bk AS site_code,
        'Unknown' AS sales_order_code,
        'Missing' AS customer_code,     --klnrc9
        'Missing' AS employee_code,     --klnrc9
        'Missing' AS payment_code,      --BETMC1
        bv_salesinvoice.currency_code,
        --The numbers below are added to help the BI team to check the data
        --WHen the missing codes are active then these numbers can be deleted.
        bv_salesinvoice.klnrc9 AS customer_number,
        bv_salesinvoice.fkrsc1 AS employee_number,
        bv_salesinvoice.betmc1 AS payment_number,
        bv_salesinvoice.rpscc1 AS replenishment_sc,
        --end test data columns
        cast(bv_salesinvoice.fkdec1 AS date) AS date_generated,
        bv_salesinvoice.fkdec1 AS datetime_generated,
        bv_salesinvoice.fanrc1 AS invoice_number,
        bv_salesinvoice.fknlc1 AS invoice_number_long,
        'Customer_Missing' AS customer_name,
        'Customer_Missing' AS vat_customer,
        'Customer_Missing' AS billing_adress,
        bv_salesinvoice.slorc1 AS distribution_channel_code,
        bv_salesinvoice.fktpc1 AS invoice_type_code,
        bv_salesinvoice.invoice_type_desc AS invoice_type,
        bv_salesinvoice.pgmtc1 AS program_code,
        bv_salesinvoice.program_desc AS program,
        bv_salesinvoice.derived_invoice_excl_vat_indicator AS invoice_excl_vat_indicator,
        lnk_invoiceheader.fktoc1 AS total_amount_incl_vat,
        --Concatination based on the site location code and the device code (cash register)
        concat(bv_salesinvoice.finrc1, '||', trim(bv_salesinvoice.dvnmc1)) AS checkout_code
    FROM {{ ref("rv_tlnk__sales_invoice_header_transactions") }} AS lnk_invoiceheader
    INNER JOIN bv_salesinvoice
        ON lnk_invoiceheader.sales_invoice_hk = bv_salesinvoice.sales_invoice_hk
    LEFT JOIN hub_site
        ON lnk_invoiceheader.site_hk = hub_site.site_hk
    --Qualify to get the latest active row for the invoice from the transactions lnk.
    QUALIFY row_number() OVER (
        PARTITION BY lnk_invoiceheader.sales_invoice_header_transactions_hk, bv_salesinvoice.start_datetime
        ORDER BY bv_salesinvoice.start_datetime ASC, bv_salesinvoice.load_datetime DESC
    ) = 1
)

SELECT
    sales_invoice_code AS pk_sales_invoice_code,
    legal_entity_code AS fk_legal_entity_code,
    site_code AS fk_site_code,
    sales_order_code AS fk_sales_order_code,
    customer_code AS fk_customer_code,
    employee_code AS fk_employee_code,
    payment_code AS fk_payment_code,
    currency_code AS fk_currency_code,
    customer_number,
    employee_number,
    payment_number,
    replenishment_sc,
    date_generated,
    datetime_generated,
    invoice_number,
    invoice_number_long,
    customer_name,
    vat_customer,
    billing_adress,
    distribution_channel_code,
    invoice_type_code,
    invoice_type,
    program_code,
    program,
    invoice_excl_vat_indicator,
    total_amount_incl_vat,
    checkout_code
FROM final

{% if is_incremental() %}
    WHERE date_generated > date_sub(current_date(), INTERVAL 7 DAY)
{% endif %}
