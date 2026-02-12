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

bdv_address AS (
    SELECT
        bdv_address.addressid,
        bdv_address.customerid,
        bdv_address.attentionof,
        bdv_address.addresstype,
        bdv_address.addressstatus,
        bdv_address.addressname,
        bdv_address.street,
        bdv_address.housenumber,
        bdv_address.housenumberaddition,
        bdv_address.postalcode,
        bdv_address.city,
        bdv_address.countrycode,
        bdv_address.emailaddress,
        bdv_address.phonenumber,
        bdv_address.phonenumbermobile,
        bdv_address.end_datetime,
        bdv_address.load_datetime,
        bdv_address.customer_hk,
        bdv_address.customer_address_hk
    FROM {{ ref('bv_bdv__customer_address') }} AS bdv_address
    WHERE
        bdv_address.end_datetime IS null
),

final AS (
    SELECT
        bdv_address.*,
        hub_customer.customer_bk
    FROM hub_customer
    INNER JOIN bdv_address
        ON hub_customer.customer_hk = bdv_address.customer_hk
    LEFT JOIN sal_customer_demoted
        ON hub_customer.customer_hk = sal_customer_demoted.demoted_customer_hk
    {# Removing demoted customers, otherwise we have duplicates in our data. #}
    WHERE sal_customer_demoted.demoted_customer_hk IS null
)

SELECT
    concat(customer_bk, '||', addresstype) AS pk_customer_address_code,
    customer_bk AS fk_customer_code,
    attentionof AS attention_of,
    addresstype AS address_type,
    addressstatus AS address_status,
    addressname AS address_name,
    street,
    housenumber AS house_number,
    housenumberaddition AS house_number_addition,
    postalcode AS postal_code,
    city,
    countrycode AS country_code,
    emailaddress AS email_address,
    phonenumber AS phone_number,
    phonenumbermobile AS phone_number_mobile
FROM final
