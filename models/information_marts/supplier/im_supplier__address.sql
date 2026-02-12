WITH

supplier_address AS (
    SELECT
        hub_supplier.supplier_bk,
        sat_supplier.lvadc1,
        sat_supplier.lvadc2,
        sat_suplier_contact.lvadc2 AS contact_adresss2,
        sat_suplier_contact.lvadc3,
        sat_supplier.lvpkc1,
        sat_suplier_contact.lvpkc2,
        sat_supplier.lvwpc1,
        sat_supplier.lvwpc2,
        sat_supplier.lakdc1
    FROM {{ ref("rv_hub__supplier") }} AS hub_supplier
    INNER JOIN {{ ref("bv_sat__supplier_general_info") }} AS sat_supplier
        ON hub_supplier.supplier_hk = sat_supplier.supplier_hk
    LEFT JOIN {{ ref("rv_sat__supplier_contacts") }} AS sat_suplier_contact
        ON hub_supplier.supplier_hk = sat_suplier_contact.supplier_hk
    WHERE
        sat_supplier.end_datetime IS null
        AND sat_suplier_contact.end_datetime IS null
),

{# Becasue every address should be an individual row
and in the source all addresses are separeated over different columns for one supplier,
I created a dataset per address type. #}
general_address AS (
    SELECT DISTINCT
        supplier_bk AS fk_supplier_code,
        'Algemeen' AS address_type,
        lvadc1 AS postal_code,
        lakdc1 AS country,
        --The coalesc is needed because of a lack of data quality in the source
        COALESCE(lvadc1, lvadc2) AS address,
        --The coalesc is needed because of a lack of data quality in the source
        COALESCE(lvwpc1, lvwpc2) AS city,
        {# a supplier can have multiple addresses, only the supplier_bk is not sufficient.
        Therefore the addition of the address_type #}
        CONCAT(supplier_bk, '||', 'Algemeen') AS pk_supplier_address_code
    FROM supplier_address
),

visit_address AS (
    SELECT DISTINCT
        supplier_bk AS fk_supplier_code,
        'Bezoek' AS address_type,
        contact_adresss2 AS address,
        'Unknown' AS country,
        --The left function is needed because of a lack of data quality in the source
        COALESCE(LEFT(lvadc3, 7), lvpkc2) AS postal_code,
        --The substring function is needed because of a lack of data quality in the source
        COALESCE(SUBSTRING(lvadc3, 8, 50), lvwpc2) AS city,
        CONCAT(supplier_bk, '||', 'Bezoek') AS pk_supplier_address_code
    FROM supplier_address
),

production_address AS (
    SELECT DISTINCT
        supplier_bk AS fk_supplier_code,
        'Productie' AS address_type,
        contact_adresss2 AS address,
        'Unknown' AS country,
        lvpkc2 AS postal_code,
        lvwpc2 AS city,
        CONCAT(supplier_bk, '||', 'Productie') AS pk_supplier_address_code
    FROM supplier_address
),

{# Becasue every contact should be an individual row
and in the source all contacts are separated over different columns for one supplier,
I created a dataset per contact type. #}
unioning AS (
    {% set list_of_columns = [
        'pk_supplier_address_code',
        'fk_supplier_code',
        'address_type',
        'address',
        'postal_code',
        'city',
        'country'
    ] -%}
    {% set list_of_ctes = ['general_address', 'visit_address', 'production_address'] -%}
    {% for cte in list_of_ctes %}
        SELECT {{ list_of_columns | join(', ') }}
        FROM {{ cte }}
        {% if not loop.last %}
            UNION DISTINCT
        {% endif %}
    {%- endfor %}
)

SELECT *
FROM unioning
