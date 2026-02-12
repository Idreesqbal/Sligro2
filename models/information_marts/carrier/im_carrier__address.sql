WITH

general_address AS (
    SELECT
        carrier_hk,
        'Algemeen' AS address_type,
        lvadc1 AS address,
        lvwpc1 AS city,
        TRIM(lvpkc1) AS postal_code,
        TRIM(lakdc1) AS country
    FROM {{ ref('bv_sat__carrier_general_info') }}
    WHERE end_datetime IS null
),

sat_carrier_contacts AS (
    SELECT *
    FROM {{ ref('bv_sat__carrier_contacts') }}
    WHERE end_datetime IS null
),

visit_address AS (
    SELECT
        carrier_hk,
        'Bezoek' AS address_type,
        lvadc2 AS address,
        'Unknown' AS country,
        lvwpc2 AS city,
        TRIM(REPLACE(lvpkc2, ' ', '')) AS postal_code --example: 3851 SL into 3851SL
    FROM sat_carrier_contacts
    WHERE
        LENGTH(TRIM(lvadc2)) > 0
        OR LENGTH(TRIM(lvpkc2)) > 0
        OR LENGTH(TRIM(lvwpc2)) > 0
),

carrier_address AS (
    {% set list_of_columns = [
        'carrier_hk',
        'address_type',
        'address',
        'country',
        'postal_code',
        'city'
    ] -%}
    {% set list_of_ctes = ['general_address', 'visit_address'] -%}
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
        CONCAT(hub_carrier.carrier_bk, '||', carrier_address.address_type) AS pk_carrier_address_code,
        hub_carrier.carrier_bk AS fk_carrier_code,
        carrier_address.address_type,
        NULLIF(carrier_address.postal_code, '') AS postal_code,
        carrier_address.country,
        carrier_address.address,
        carrier_address.city
    FROM {{ ref('rv_hub__carrier') }} AS hub_carrier
    INNER JOIN carrier_address
        ON hub_carrier.carrier_hk = carrier_address.carrier_hk
)

SELECT *
FROM result
