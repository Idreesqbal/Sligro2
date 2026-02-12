{{ config(
    materialized='table'
) }}

WITH

sat_carrier_general_info AS (
    SELECT *
    FROM {{ ref('bv_sat__carrier_general_info') }}
    WHERE end_datetime IS null
),

sat_carrier_contacts AS (
    SELECT *
    FROM {{ ref('bv_sat__carrier_contacts') }}
    WHERE end_datetime IS null
),

representer AS (
    SELECT
        carrier_hk,
        'Algemeen' AS address_type,
        'Vertegenwoordiger' AS contact_type,
        vertc1 AS contact_name,
        emalc1 AS email_address,
        TRIM(tlnrc1) AS phone_number
    FROM sat_carrier_general_info
    WHERE
        LENGTH(TRIM(vertc1)) > 0
        OR LENGTH(TRIM(tlnrc1)) > 0
        OR LENGTH(TRIM(emalc1)) > 0
),

-- Due to the structure in AS400 table, all contact persons found in sat_carrier_contacts is linked to 'Bezoek' address
account_manager AS (
    SELECT
        carrier_hk,
        'Bezoek' AS address_type,
        'Account Manager' AS contact_type,
        accmc1 AS contact_name,
        emalc5 AS email_address,
        TRIM(tlnrc5) AS phone_number
    FROM sat_carrier_contacts
    WHERE
        LENGTH(TRIM(accmc1)) > 0
        OR LENGTH(TRIM(tlnrc5)) > 0
        OR LENGTH(TRIM(emalc5)) > 0
),

order_contact AS (
    SELECT
        carrier_hk,
        'Bezoek' AS address_type,
        fomsc1 AS contact_type,
        cntpc1 AS contact_name,
        emalc2 AS email_address,
        TRIM(tlnrc2) AS phone_number
    FROM sat_carrier_contacts
    WHERE
        LENGTH(TRIM(fomsc1)) > 0
        OR LENGTH(TRIM(cntpc1)) > 0
        OR LENGTH(TRIM(tlnrc2)) > 0
        OR LENGTH(TRIM(emalc2)) > 0
),

invoice_contact AS (
    SELECT
        carrier_hk,
        'Bezoek' AS address_type,
        fomsc2 AS contact_type,
        cntpc2 AS contact_name,
        emalc3 AS email_address,
        TRIM(tlnrc3) AS phone_number
    FROM sat_carrier_contacts
    WHERE
        LENGTH(TRIM(fomsc2)) > 0
        OR LENGTH(TRIM(cntpc2)) > 0
        OR LENGTH(TRIM(tlnrc3)) > 0
        OR LENGTH(TRIM(emalc3)) > 0
),

quality_contact AS (
    SELECT DISTINCT
        carrier_hk,
        'Bezoek' AS address_type,
        fomsc3 AS contact_type,
        cntpc3 AS contact_name,
        emalc4 AS email_address,
        TRIM(tlnrc4) AS phone_number
    FROM sat_carrier_contacts
    WHERE
        LENGTH(TRIM(fomsc3)) > 0
        OR LENGTH(TRIM(cntpc3)) > 0
        OR LENGTH(TRIM(tlnrc4)) > 0
        OR LENGTH(TRIM(emalc4)) > 0
),

carrier_contact_person AS (
    {% set list_of_columns = [
        'carrier_hk',
        'address_type',
        'contact_type',
        'contact_name',
        'phone_number',
        'email_address'
    ] -%}
    {% set list_of_ctes = ['representer', 'account_manager', 'order_contact', 'invoice_contact', 'quality_contact'] -%}
    {% for cte in list_of_ctes %}
        SELECT {{ list_of_columns | join(', ') }}
        FROM {{ cte }}
        {% if not loop.last %}
            UNION DISTINCT
        {% endif %}
    {%- endfor %}
),

result AS (
    SELECT
        CONCAT(
            hub_carrier.carrier_bk, '||', carrier_contact_person.address_type, '||', carrier_contact_person.contact_type
        ) AS pk_carrier_contact_person_code,
        CONCAT(hub_carrier.carrier_bk, '||', carrier_contact_person.address_type) AS fk_carrier_address_code,
        hub_carrier.carrier_bk AS fk_carrier_code,
        carrier_contact_person.contact_type,
        NULLIF(carrier_contact_person.contact_name, '') AS contact_name,
        NULLIF(carrier_contact_person.phone_number, '') AS phone_number,
        NULLIF(carrier_contact_person.email_address, '') AS email_address
    FROM {{ ref('rv_hub__carrier') }} AS hub_carrier
    INNER JOIN carrier_contact_person
        ON hub_carrier.carrier_hk = carrier_contact_person.carrier_hk
)

SELECT *
FROM result
