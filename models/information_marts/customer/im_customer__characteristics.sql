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

bdv_characteristics AS (
    SELECT
        characteristictype,
        characteristiccode,
        characteristicvalue,
        is_deleted,
        load_datetime,
        end_datetime,
        customerid,
        customer_hk
    FROM {{ ref('bv_bdv__customer_characteristics') }}
    WHERE end_datetime IS null
),

final AS (
    SELECT
        bdv_characteristics.*,
        hub_customer.customer_bk
    FROM hub_customer
    INNER JOIN bdv_characteristics
        ON hub_customer.customer_hk = bdv_characteristics.customer_hk
    LEFT JOIN sal_customer_demoted
        ON hub_customer.customer_hk = sal_customer_demoted.demoted_customer_hk
    {# Removing demoted customers, otherwise we have duplicates in our data. #}
    WHERE sal_customer_demoted.demoted_customer_hk IS null
)

SELECT
    CONCAT(customer_bk, '||', characteristictype, '||', characteristiccode) AS pk_customer_characteristic_code,
    customer_bk AS fk_customer_code,
    characteristictype AS characteristic_type,
    characteristiccode AS characteristic_code,
    characteristicvalue AS characteristic_value
FROM final
