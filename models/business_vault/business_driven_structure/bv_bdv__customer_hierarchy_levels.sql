{#
The levels have the following rules given by the Business:
Level 0: the customer is not in the hierarchy, it doesn't have a parent and it is not a parent.
Level 1: is the ultimate parent.
Level 2: is the child of the ultimate parent.
Level 3....Level 9: the subsequent levels.
#}

WITH unique_key_hierarchy AS (
    SELECT *
    FROM {{ ref('bv_bdv__customer_hierarchy') }}
    WHERE end_datetime IS null
),

{# Check for parents. #}
parent_check AS (
    SELECT DISTINCT customer_parent_hk
    FROM unique_key_hierarchy
    WHERE customer_parent_hk IS NOT null
),

{# Create two indicators to check if the customer is a parent or has a parent. #}
base_customers AS (
    SELECT
        customer.customer_bk,
        customer.customer_hk,
        hierarchy.customer_parent_bk,
        hierarchy.customer_parent_hk,
        IF(p.customer_parent_hk IS NOT null, true, false) AS is_parent,
        IF(hierarchy.customer_parent_bk IS NOT null, true, false) AS has_parent
    {# Join with the customer hub to retrieve the full list of customers, not only those in the hierarchy. #}
    FROM {{ ref('bv_hub__customer') }} AS customer
    LEFT JOIN unique_key_hierarchy AS hierarchy
        ON customer.customer_hk = hierarchy.customer_hk
    LEFT JOIN parent_check AS p
        ON customer.customer_hk = p.customer_parent_hk
),

{# If the customer is a parent and doesn't have a parent, it is an ultimate parent. #}
ultimate_parents AS (
    SELECT
        customer_bk,
        customer_bk AS customer_parent_bk,
        1 AS level,
        customer_hk,
        customer_hk AS customer_parent_hk
    FROM base_customers
    WHERE NOT has_parent AND is_parent
),

{# Level 0: Customers not in any hierarchy (not a parent, not a child) #}
level_0 AS (
    SELECT
        customer_bk,
        '' AS customer_parent_bk,
        0 AS level,
        customer_hk,
        customer_parent_hk
    FROM base_customers
    WHERE NOT has_parent AND NOT is_parent
),

{# From this point of the code, we start calculating the parent of the parent of the parent. #}
level_1 AS (
    SELECT
        customer_bk,
        customer_parent_bk,
        1 AS level,
        customer_hk,
        customer_parent_hk
    FROM unique_key_hierarchy
    WHERE customer_parent_bk IS NOT null
),

{% set max_level = 10 %}

{% for i in range(2, max_level + 1) %}
    level_{{ i }} AS (
        SELECT
            level_{{ i - 1 }}.customer_bk,
            level_{{ i }}.customer_parent_bk,
            level_{{ i - 1 }}.customer_hk,
            level_{{ i }}.customer_parent_hk,
            {{ i }} AS level
        FROM level_{{ i - 1 }}
        INNER JOIN unique_key_hierarchy AS level_{{ i }}
            ON level_{{ i - 1 }}.customer_parent_bk = level_{{ i }}.customer_bk
        WHERE level_{{ i }}.customer_parent_bk IS NOT null
    ),
{% endfor %}

{# In the final result, we join the ultimate parent, with level 0 and the other levels. #}
final_result AS (
    SELECT
        customer_bk,
        customer_parent_bk,
        level,
        customer_hk,
        customer_parent_hk
    FROM level_0
    UNION ALL
    SELECT
        customer_bk,
        customer_parent_bk,
        level,
        customer_hk,
        customer_parent_hk
    FROM ultimate_parents
    UNION ALL
    SELECT
        customer_bk,
        customer_parent_bk,
        level,
        customer_hk,
        customer_parent_hk
    FROM level_1
    {% for i in range(2, max_level + 1) %}
        UNION ALL
        SELECT
            customer_bk,
            customer_parent_bk,
            level,
            customer_hk,
            customer_parent_hk
        FROM level_{{ i }}
    {% endfor %}
),

{# When we calculate the levels, the order is flipped, so we need to reverse the logic of the level count. #}
flipped_levels AS (
    SELECT
        customer_bk,
        customer_parent_bk,
        level,
        MAX(level) OVER (PARTITION BY customer_hk) AS max_level,
        customer_hk,
        customer_parent_hk
    FROM final_result
),

{# Final level should be the customer bk itself #}
with_self_reference AS (
    SELECT
        customer_bk,
        customer_parent_bk,
        IF(max_level = 0, 0, (max_level - level + 1)) AS level,
        customer_hk,
        customer_parent_hk
    FROM flipped_levels

    UNION ALL

    SELECT
        flipped_levels.customer_bk,
        flipped_levels.customer_bk AS customer_parent_bk,
        flipped_levels.max_level + 1 AS level,
        flipped_levels.customer_hk,
        flipped_levels.customer_hk AS customer_parent_hk
    FROM flipped_levels
    LEFT JOIN ultimate_parents ON flipped_levels.customer_bk = ultimate_parents.customer_parent_bk
    WHERE flipped_levels.level = 1 AND ultimate_parents.customer_parent_bk IS null
)

SELECT
    customer_bk,
    customer_parent_bk,
    level,
    customer_hk,
    customer_parent_hk
FROM with_self_reference
