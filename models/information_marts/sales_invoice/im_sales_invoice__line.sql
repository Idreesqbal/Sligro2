{{ config(
    unique_key = 'pk_sales_invoice_line_code'
    , partition_by={
        'field': 'invoice_line_date',
        'data_type': 'datetime'}
    , on_schema_change='append_new_columns'
) }}

--collect the sales invoice BK
WITH

salesinvoice_lines AS (
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
        derived_transaction_type,
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
        net_amount,
        cost_value,
        gross_value,
        zb_value,
        discount_per_unit,
        discount_value,
        derived_vat_amount,
        btw_percentage,
        btw_oms,
        start_datetime,
        load_datetime
    FROM {{ ref("bv_bdv__sales_invoice_line") }}
),

--Single CTE to get all the information from article.
--At this point the combination with trade item is not working and being fixed 27-1-2025
hub_article AS (
    SELECT
        hub_article.article_hk,
        hub_article.article_bk,
        sat_article_details.aromc1,
        sat_trade_item.numberofbaseunits,
        sat_trade_item.unitofmeasure
    FROM {{ ref("rv_hub__article") }} AS hub_article
    LEFT JOIN {{ ref("rv_sat__article_AS400_details") }} AS sat_article_details
        ON hub_article.article_hk = sat_article_details.article_hk
    LEFT JOIN {{ ref("rv_lnk__article__trade_item") }} AS lnk_article_trade_item
        ON hub_article.article_hk = lnk_article_trade_item.article_hk
    LEFT JOIN {{ ref("rv_sat__trade_item") }} AS sat_trade_item
        ON lnk_article_trade_item.trade_item_hk = sat_trade_item.trade_item_hk
    WHERE
        sat_article_details.end_datetime IS null
        AND sat_trade_item.end_datetime IS null
),

--collect the promotion bk
hub_promotion AS (
    SELECT
        promotion_hk,
        promotion_bk
    FROM {{ ref("rv_hub__promotion") }}
),

final AS (
    SELECT
        salesinvoice_lines.sales_invoice_line_code,
        salesinvoice_lines.sales_invoice_bk AS sales_invoice_code,
        -- hub_article.article_bk Filtert out
        -- becasue Connection challenges at article give null as a result at this point
        salesinvoice_lines.arnrc1 AS article_code,
        hub_promotion.promotion_bk AS promotion_code,
        'Missing' AS price_code,
        --The numbers below are added to help the BI team to check the data
        --WHen the missing codes are active then these numbers can be deleted.
        salesinvoice_lines.prnrc1 AS price_number,
        salesinvoice_lines.btkdc1 AS vat_code,
        --end test data columns
        salesinvoice_lines.psnrc1 AS line_position,
        salesinvoice_lines.fkdec1 AS invoice_line_date,
        salesinvoice_lines.arnrc1 AS article_number,
        hub_article.numberofbaseunits AS number_of_base_units,
        --hub_article.unitofmeasure Filtert out
        --becasue Connection challenges at article give null as a result at this point
        'Article_Missing' AS unit_of_measure,
        --hub_article.aromc1 Filtert out becasue Connection challenges at article give null as a result at this point
        'Article_Missing' AS article_name,
        'Article_Missing' AS variant,
        'Article_Missing' AS article_group, --sat_details.ARGRC1?
        'Article_Missing' AS comments,
        'Article_Missing' AS lotnumber,
        salesinvoice_lines.derived_transaction_type AS transaction_type, --TDGP-1942: Replaces data router_code logic
        salesinvoice_lines.klnuc1 AS concessionaire_number,
        salesinvoice_lines.spnrc1 AS price_type,
        salesinvoice_lines.zbbxc1 AS net_price,
        salesinvoice_lines.brpxc1 AS gross_price,
        salesinvoice_lines.fkaac1 AS quantity,
        salesinvoice_lines.koprc1 AS cost_price,
        salesinvoice_lines.zbprc1 AS zb_price,
        salesinvoice_lines.kopcc1 AS discount_percentage,
        salesinvoice_lines.derived_vat_amount AS vat_amount,
        salesinvoice_lines.btw_percentage AS vat_rate,
        salesinvoice_lines.btw_oms AS vat_description,
        salesinvoice_lines.net_amount,
        salesinvoice_lines.cost_value,
        salesinvoice_lines.gross_value,
        salesinvoice_lines.zb_value,
        salesinvoice_lines.discount_per_unit,
        salesinvoice_lines.discount_value
    FROM salesinvoice_lines
    LEFT JOIN hub_article
        ON salesinvoice_lines.article_hk = hub_article.article_hk
    LEFT JOIN hub_promotion
        ON salesinvoice_lines.promotion_hk = hub_promotion.promotion_hk
)

SELECT
    sales_invoice_line_code AS pk_sales_invoice_line_code,
    sales_invoice_code AS fk_sales_invoice_code,
    article_code AS fk_article_code,
    promotion_code AS fk_promotion_code,
    price_code AS fk_price_code,
    transaction_type, --TDGP-1942: replaces the data_router_code,
    vat_amount,
    vat_rate,
    vat_description,
    price_number,
    vat_code,
    line_position,
    invoice_line_date,
    article_number,
    number_of_base_units,
    unit_of_measure,
    article_name,
    variant,
    article_group,
    comments,
    lotnumber,
    concessionaire_number,
    price_type,
    net_price,
    gross_price,
    quantity,
    cost_price,
    zb_price,
    discount_percentage,
    net_amount,
    cost_value,
    gross_value,
    zb_value,
    discount_per_unit,
    discount_value
FROM final

{% if is_incremental() %}
    WHERE invoice_line_date > DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
{% endif %}
