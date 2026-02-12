WITH

tlink_stock_transactions_cdc__trim_mutation_code AS (
    SELECT
        mtdtgb,
        article_bk,
        sales_order_nk,
        article_hk,
        article_group_hk,
        supplier_hk,
        aantgb,
        TRIM(kmkdgb) AS kmkdgb
    FROM {{ ref('rv_tlnk__stock_transactions_cdc') }}
),

stock_transactions_cdc AS (
    SELECT
        null AS fk_stock_location_code,
        tlink_stock_transactions_cdc.article_bk AS fk_article_code,
        tlink_stock_transactions_cdc.sales_order_nk AS fk_sales_order_code,
        tlink_stock_transactions_cdc.kmkdgb AS mutation_code,
        'MISSING_UNKNOWN' AS mutation_description,
        PARSE_DATE('%y%m%d', CAST(tlink_stock_transactions_cdc.mtdtgb AS STRING)) AS transaction_date,
        'MISSING_UNKNOWN' AS transaction_weight,
        CONCAT(
            tlink_stock_transactions_cdc.mtdtgb, '||',
            'null', '||',
            tlink_stock_transactions_cdc.article_bk, '||',
            tlink_stock_transactions_cdc.kmkdgb, '||',
            tlink_stock_transactions_cdc.sales_order_nk
        ) AS pk_transaction_code,
        SUM(tlink_stock_transactions_cdc.aantgb) AS transaction_quantity
    FROM tlink_stock_transactions_cdc__trim_mutation_code AS tlink_stock_transactions_cdc
    INNER JOIN {{ ref('rv_hub__article_AS400') }} AS hub_article
        ON tlink_stock_transactions_cdc.article_hk = hub_article.article_hk
    GROUP BY
        tlink_stock_transactions_cdc.mtdtgb,
        tlink_stock_transactions_cdc.article_bk,
        tlink_stock_transactions_cdc.kmkdgb,
        tlink_stock_transactions_cdc.sales_order_nk
),

unioning AS (
    {% set list_of_columns = [
        'pk_transaction_code',
        'fk_stock_location_code',
        'fk_article_code',
        'fk_sales_order_code',
        'mutation_code',
        'mutation_description',
        'transaction_date',
        'transaction_weight',
        'transaction_quantity'
    ] -%}
    {% set list_of_ctes = ['stock_transactions_cdc'] -%}
    {% for cte in list_of_ctes %}
        SELECT {{ list_of_columns | join(', ') }}
        FROM {{ cte }}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {%- endfor %}
)

SELECT *
FROM unioning
