WITH lnk_header AS (
    SELECT
        purchase_invoice_header_transactions_hk,
        purchase_invoice_hk,
        supplier_hk,
        babdif,
        bbbdif,
        fkbdif,
        vafkif,
        embdif,
        toioif
    FROM {{ ref('rv_tlnk__purchase_invoice_header_transactions') }}
),

status_description AS (
    SELECT
        field_name,
        field_value,
        field_value_description
    FROM {{ ref('rv_sat__purchase_invoice_status_omschrijving') }}
),

hub_header AS (
    SELECT
        purchase_invoice_hk,
        purchase_invoice_bk
    FROM {{ ref('rv_hub__purchase_invoice') }}
),

sat_header AS (
    SELECT
        purchase_invoice_hk,
        stkdif,
        fkdtif,
        ovdtif,
        vfdtif,
        fknrif,
        vakdif,   -- remove when currency_bk is connected from currency entity
        jfdtif    -- approval date
    FROM {{ ref('bv_sat__purchase_invoice_header') }}
),

supplier_general_latest AS (
    SELECT
        supplier_hk,
        btnrc1
    FROM {{ ref('bv_sat__supplier_general_info') }}
    QUALIFY ROW_NUMBER()
        OVER (
            PARTITION BY supplier_hk
            ORDER BY end_datetime DESC NULLS LAST
        )
    = 1
),

supplier_finance_latest AS (
    SELECT
        supplier_hk,
        ibanc1
    FROM {{ ref('rv_sat__supplier_finance') }}
    QUALIFY ROW_NUMBER()
        OVER (
            PARTITION BY supplier_hk
            ORDER BY end_datetime DESC NULLS LAST
        )
    = 1
),

supplier AS (
    SELECT
        h.supplier_hk,
        h.supplier_bk,
        g.btnrc1 AS supplier_vat_number,
        f.ibanc1 AS supplier_bankaccount_number
    FROM {{ ref('rv_hub__supplier') }} AS h
    LEFT JOIN supplier_general_latest AS g
        ON h.supplier_hk = g.supplier_hk
    LEFT JOIN supplier_finance_latest AS f
        ON h.supplier_hk = f.supplier_hk
),


finance_cal AS (
    SELECT
        calendar_date_nk AS pk_calendar_date_code,
        year_number AS financial_year,
        month_of_year AS financial_period
    FROM {{ ref('ref__calendar') }}
),

final AS (
    SELECT
        hh.purchase_invoice_bk AS purchase_invoice_code,
        s.supplier_bk AS supplier_code,
        'Missing' AS legal_entity_code,
        sh.vakdif AS currency,
        s.supplier_vat_number,
        s.supplier_bankaccount_number,
        sh.stkdif AS status,
        sd.field_value_description AS status_description,
        CASE
            WHEN lh.fkbdif < 0 THEN 'Credit'
            WHEN lh.fkbdif > 0 THEN 'Debet'
        END AS purchase_invoice_type,
        'Unknown' AS payment_terms,
        sh.fkdtif AS created_date,
        sh.ovdtif AS received_date,
        sh.vfdtif AS due_date,
        sh.fknrif AS supplier_invoice_code,
        sh.jfdtif AS approval_date,
        fc.financial_year,
        fc.financial_period,
        lh.toioif AS total_amount_excluding_vat,
        CASE
            WHEN lh.babdif <> 0 THEN lh.babdif
            WHEN lh.bbbdif <> 0 THEN lh.bbbdif
            ELSE lh.babdif
        END AS vat_amount,
        lh.fkbdif AS total_amount_including_vat,
        lh.vafkif AS deductables,
        lh.embdif AS packaging_value
    FROM lnk_header AS lh
    INNER JOIN hub_header AS hh
        ON lh.purchase_invoice_hk = hh.purchase_invoice_hk
    LEFT JOIN sat_header AS sh
        ON hh.purchase_invoice_hk = sh.purchase_invoice_hk
    LEFT JOIN status_description AS sd
        ON CAST(sd.field_value AS INT) = CAST(sh.stkdif AS INT)
    LEFT JOIN supplier AS s
        ON lh.supplier_hk = s.supplier_hk
    LEFT JOIN finance_cal AS fc
        ON sh.ovdtif = fc.pk_calendar_date_code
)

SELECT DISTINCT
    purchase_invoice_code AS pk_purchase_invoice_code,
    supplier_code AS fk_supplier_code,
    -- purchase_order_code       AS fk_purchase_order_code,  -- removed
    legal_entity_code AS fk_legal_entity_code,
    NULLIF(currency, '') AS fk_currency_code,
    status,
    status_description,
    purchase_invoice_type,
    payment_terms,
    approval_date,
    created_date,
    received_date,
    due_date,
    supplier_invoice_code,
    total_amount_including_vat,
    total_amount_excluding_vat,
    vat_amount,
    deductables,
    packaging_value
FROM final
ORDER BY pk_purchase_invoice_code DESC
