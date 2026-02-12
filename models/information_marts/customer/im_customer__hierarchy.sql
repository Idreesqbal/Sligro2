{% set max_level = 9 %}

WITH full_customers AS (
    SELECT *
    FROM {{ ref('bv_bdv__customer_customer') }}
    WHERE end_datetime IS null
),

customer_hierarchy AS (
    SELECT
        hierarchy.customer_bk,
        hierarchy.customer_hk,
        hierarchy.customer_parent_bk,
        hierarchy.customer_parent_hk,
        hierarchy.level,
        customer_detail.tradename AS customer_trade_name,
        customer_detail.customerid AS customer_code,
        customer_detail_parent.tradename AS parent_trade_name,
        customer_detail_parent.customerid AS parent_customer_code
    FROM {{ ref('bv_bdv__customer_hierarchy_levels') }} AS hierarchy
    LEFT JOIN full_customers AS customer_detail
        ON hierarchy.customer_hk = customer_detail.customer_hk
    LEFT JOIN full_customers AS customer_detail_parent
        ON hierarchy.customer_parent_hk = customer_detail_parent.customer_hk
),

hub_customer AS (
    SELECT
        customer_bk,
        customer_hk
    FROM {{ ref('bv_hub__customer') }}
),

sal_customer_demoted AS (
    SELECT
        customer_hk,
        demoted_customer_hk
    FROM {{ ref('bv_salnk__customer__demoted_customer') }}
)

{# As per request of the BI team, this table has been flattened,
containing all the levels for a given customer in the same row. #}
SELECT
    hc.customer_bk AS pk_customer_code,
    ch.customer_trade_name AS trade_name,
    ch.customer_code,
    max(if(ch.level IS null, 0, ch.level)) AS level,
    {% for i in range(1, max_level + 1) %}
        max(if(ch.level = {{ i }}, ch.customer_parent_bk, null))
            AS fk_customer_parent_code_level_{{ i }},
        max(if(ch.level = {{ i }}, ch.parent_customer_code, null))
            AS customer_parent_code_level_{{ i }},
        max(if(ch.level = {{ i }}, ch.parent_trade_name, null))
            AS parent_trade_name_level_{{ i }}
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}

FROM hub_customer AS hc
LEFT JOIN customer_hierarchy AS ch
    ON hc.customer_bk = ch.customer_bk
LEFT JOIN sal_customer_demoted
    ON hc.customer_hk = sal_customer_demoted.demoted_customer_hk
{# Removing demoted customers, otherwise we have duplicates in our data. #}
WHERE sal_customer_demoted.demoted_customer_hk IS null
GROUP BY hc.customer_bk, hc.customer_hk, ch.customer_trade_name, ch.customer_code
