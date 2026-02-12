WITH lnk_lines AS (
    SELECT
        purchase_invoice_lines_transactions_hk,
        purchase_invoice_hk,
        bdrgc1,
        bdrgc2,
        bdrgc3,
        bdrgc4,
        bdrgc5,
        bdrgc6,
        bdrgc8,
        fcaac1
    FROM `sfg-data-warehouse-cprod-hjkc`.`dbt_idrees_raw_vault`.`rv_tlnk__purchase_invoice_lines_transactions`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY purchase_invoice_lines_transactions_hk
        ORDER BY load_datetime DESC
    ) = 1
),

hub_lines AS (
    SELECT
        purchase_invoice_hk,
        purchase_invoice_bk
    FROM `sfg-data-warehouse-cprod-hjkc`.`dbt_idrees_raw_vault`.`rv_hub__purchase_invoice`
),

sat_lines AS (
    SELECT
        purchase_invoice_lines_transactions_hk,
        stkdc1,
        rgnrc3,
        ean1c1,
        arnrc1
    FROM `sfg-data-warehouse-cprod-hjkc`.`dbt_idrees_raw_vault`.`rv_sat__purchase_invoice_line`
    WHERE end_datetime IS null
),

-- NEW: Pull Purchase-Order per Invoice (same logic that used to live in header)
purchase_order_ln AS (
    SELECT
        hub_purchase_order.purchase_order_bk,
        lnk_po.purchase_invoice_hk
    FROM
        `sfg-data-warehouse-cprod-hjkc`.`dbt_idrees_raw_vault`.`rv_lnk__relation__purchase_invoice__purchase_order`
            AS lnk_po
    INNER JOIN
        `sfg-data-warehouse-cprod-hjkc`.`dbt_idrees_raw_vault`.`rv_sat__relation__purchase_invoice__purchase_order`
            AS sat_po
        ON
            lnk_po.relation__purchase_invoice__purchase_order_hk
            = sat_po.relation__purchase_invoice__purchase_order_hk
    INNER JOIN
        `sfg-data-warehouse-cprod-hjkc`.`dbt_idrees_raw_vault`.`rv_hub__purchase_order` AS rv_po
        ON lnk_po.purchase_order_hk = rv_po.purchase_order_hk
    INNER JOIN
        `sfg-data-warehouse-cprod-hjkc`.`dbt_idrees_business_vault`.`bv_bdv__purchase_order` AS hub_purchase_order
        ON
            rv_po.purchase_order_bk = hub_purchase_order.ionrba
            AND sat_po.start_datetime BETWEEN
            hub_purchase_order.start_datetime
            AND COALESCE(hub_purchase_order.end_datetime, "9999-12-31")
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            lnk_po.purchase_invoice_hk,
            hub_purchase_order.purchase_order_bk,
            sat_po.start_datetime
        ORDER BY
            sat_po.start_datetime ASC,
            lnk_po.load_datetime DESC
    ) = 1
),

final AS (
    SELECT
        CONCAT(
            hub_lines.purchase_invoice_bk,
            "||",
            sat_lines.rgnrc3,
            "||",
            sat_lines.stkdc1
        ) AS purchase_invoice_line_code,

        hub_lines.purchase_invoice_bk AS purchase_invoice_code,  -- STNRC2

        -- JOIN directly to purchase_order_ln instead of pulling from header
        purchase_order_ln.purchase_order_bk AS fk_purchase_order_line_code,

        "Missing" AS cost_centre_code,
        "Missing" AS article_code,
        "Missing" AS general_ledger_code,

        sat_lines.arnrc1 AS article_number,
        lnk_lines.fcaac1 AS quantity,
        "Unknown" AS unit_of_measure,
        "Article_missing" AS article_description,
        "Unknown" AS vat_rate,
        lnk_lines.bdrgc1 AS total_amount,
        lnk_lines.bdrgc2 AS goods_amount,
        lnk_lines.bdrgc3 AS deductables,
        lnk_lines.bdrgc4 AS surcharges,
        lnk_lines.bdrgc5 AS vat_amount,
        lnk_lines.bdrgc6 AS packaging_amount,
        lnk_lines.bdrgc8 AS packaging_compensation_amount

    FROM lnk_lines
    INNER JOIN hub_lines
        ON lnk_lines.purchase_invoice_hk = hub_lines.purchase_invoice_hk

    INNER JOIN sat_lines
        ON
            lnk_lines.purchase_invoice_lines_transactions_hk
            = sat_lines.purchase_invoice_lines_transactions_hk

    LEFT JOIN purchase_order_ln
        ON hub_lines.purchase_invoice_hk = purchase_order_ln.purchase_invoice_hk
)

SELECT
    purchase_invoice_line_code AS pk_purchase_invoice_line_code,
    purchase_invoice_code AS fk_purchase_invoice_code,
    fk_purchase_order_line_code,
    cost_centre_code AS fk_cost_centre_code,
    article_code AS fk_article_code,
    general_ledger_code AS fk_general_ledger_code,
    article_number,
    quantity,
    unit_of_measure,
    article_description,
    vat_rate,
    total_amount,
    goods_amount,
    deductables,
    surcharges,
    vat_amount,
    packaging_amount,
    packaging_compensation_amount
FROM final
