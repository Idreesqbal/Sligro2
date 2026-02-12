WITH

hub_customer AS (
    SELECT
        customer_hk,
        customer_bk
    FROM {{ ref("bv_hub__customer") }}
),

sal_customer_demoted AS (
    SELECT
        customer_hk,
        demoted_customer_hk
    FROM {{ ref('bv_salnk__customer__demoted_customer') }}
),

bdv_customer_group AS (
    SELECT
        customerid,
        customer_hk,
        customer_group_bk,
        description,
        groupid,
        typecode,
        typedescription,
        is_deleted,
        load_datetime,
        end_datetime
    FROM {{ ref('bv_bdv__customer_groups') }}
    WHERE end_datetime IS null
),

final AS (
    SELECT
        bdv_customer_group.*,
        hub_customer.customer_bk
    FROM hub_customer
    INNER JOIN bdv_customer_group
        ON hub_customer.customer_hk = bdv_customer_group.customer_hk
    LEFT JOIN sal_customer_demoted
        ON hub_customer.customer_hk = sal_customer_demoted.demoted_customer_hk
    {# Removing demoted customers, otherwise we have duplicates in our data. #}
    WHERE sal_customer_demoted.demoted_customer_hk IS null
)

SELECT
    customer_group_bk AS pk_customer_group_code,
    customer_bk AS fk_customer_code,
    groupid AS group_code,
    description,
    typecode AS type_code,
    typedescription AS type_description
FROM final
