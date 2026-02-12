WITH

hub_customer AS (
    SELECT
        customer_hk,
        customer_bk
    FROM {{ ref("bv_hub__customer") }}
),

hub_customer_contact_person AS (
    SELECT
        contact_person_hk,
        contact_person_bk
    FROM {{ ref("bv_hub__customer_contact_person") }}
),

sal_customer_demoted AS (
    SELECT
        customer_hk,
        demoted_customer_hk
    FROM {{ ref('bv_salnk__customer__demoted_customer') }}
),

bdv_customer_contact_person AS (
    SELECT
        customerid,
        contactpersonid,
        countrylanguagecode,
        state,
        contactpersontype,
        initials,
        firstname,
        preposition,
        lastname,
        salutation,
        sex,
        dateofbirth,
        emailaddress,
        phonenumberwork,
        phonenumberhome,
        phonenumbermobile,
        street,
        housenumber,
        housenumberaddition,
        postalcode,
        city,
        countrycode,
        jobposition,
        end_datetime,
        load_datetime,
        customer_hk,
        contact_person_hk
    FROM {{ ref('bv_bdv__customer_contact_person') }}
    WHERE end_datetime IS null

),

final AS (
    SELECT
        bdv_customer_contact_person.*,
        hub_customer.customer_bk,
        hub_customer_contact_person.contact_person_bk
    FROM hub_customer
    INNER JOIN bdv_customer_contact_person ON hub_customer.customer_hk = bdv_customer_contact_person.customer_hk
    INNER JOIN
        hub_customer_contact_person
        ON bdv_customer_contact_person.contact_person_hk = hub_customer_contact_person.contact_person_hk
    LEFT JOIN sal_customer_demoted ON hub_customer.customer_hk = sal_customer_demoted.demoted_customer_hk
    {# Removing demoted customers, otherwise we have duplicates in our data. #}
    WHERE sal_customer_demoted.demoted_customer_hk IS null
)

SELECT
    contact_person_bk AS pk_contact_person_code,
    customer_bk AS fk_customer_code,
    contactpersonid AS contact_person_code,
    countrylanguagecode AS country_language_code,
    state,
    contactpersontype AS contact_person_type,
    initials,
    firstname AS first_name,
    preposition,
    lastname AS last_name,
    TRIM(CONCAT(COALESCE(firstname, ' '), ' ', COALESCE(preposition, ' '), ' ', COALESCE(lastname, ' '))) AS full_name,
    salutation,
    sex,
    dateofbirth AS date_of_birth,
    emailaddress AS email_address,
    phonenumberwork AS phone_number_work,
    phonenumberhome AS phone_number_home,
    phonenumbermobile AS phone_number_mobile,
    street,
    housenumber AS house_number,
    housenumberaddition AS house_number_addition,
    postalcode AS postal_code,
    city,
    countrycode AS country_code,
    jobposition AS job_position
FROM final
