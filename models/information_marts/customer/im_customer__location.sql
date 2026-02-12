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

hub_customer_address AS (
    SELECT
        customer_address_hk,
        customer_address_bk
    FROM {{ ref("bv_hub__customer_address") }}
),

bdv_location AS (
    SELECT
        customer_address_hk,
        customer_hk,
        addressid,
        attentionof,
        emailaddress,
        locationname,
        phonenumber,
        is_created,
        is_deleted,
        end_datetime,
        load_datetime
    FROM {{ ref('bv_bdv__customer_location') }}
    WHERE
        end_datetime IS null
),

bdv_address AS (
    SELECT
        customer_address_hk,
        customer_hk,
        addressid,
        addresstype
    FROM {{ ref('bv_bdv__customer_address') }}
    WHERE
        end_datetime IS null
),

final AS (
    SELECT
        bdv_location.*,
        hub_customer.customer_bk,
        hub_customer_address.customer_address_bk,
        bdv_address.addresstype
    FROM hub_customer
    INNER JOIN bdv_location
        ON hub_customer.customer_hk = bdv_location.customer_hk
    INNER JOIN hub_customer_address
        ON bdv_location.customer_address_hk = hub_customer_address.customer_address_hk
    LEFT JOIN bdv_address
        ON
            hub_customer.customer_hk = bdv_address.customer_hk
            AND hub_customer_address.customer_address_hk = bdv_address.customer_address_hk
    LEFT JOIN sal_customer_demoted
        ON hub_customer.customer_hk = sal_customer_demoted.demoted_customer_hk
    {# Removing demoted customers, otherwise we have duplicates in our data. #}
    WHERE sal_customer_demoted.demoted_customer_hk IS null
)

SELECT
    CONCAT(customer_bk, '||', addresstype) AS pk_customer_address_code,
    customer_bk AS fk_customer_code,
    attentionof AS attention_of,
    emailaddress AS email_address,
    locationname AS location_name,
    phonenumber AS phone_number
FROM final
