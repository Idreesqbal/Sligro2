WITH

masat_supplier_purchasers AS (
    SELECT
        supplier_hk,
        employee_bk,
        hoofdinkoper,
        landcode
    FROM {{ ref("rv_masat__supplier_purchasers") }}
    WHERE end_datetime IS null
),

final AS (
    SELECT
        hub_supplier.supplier_bk AS fk_supplier_code,
        masat_supplier_purchasers.employee_bk AS fk_employee_code,
        'Employee Missing' AS buyer,
        masat_supplier_purchasers.landcode AS country,
        masat_supplier_purchasers.hoofdinkoper AS headbuyer,
        {# a supplier can have multiple buyers but only one per country.
        Because of multiple countries only the supplier_bk is not sufficient.
        Therefore the addition of the country #}
        CONCAT(hub_supplier.supplier_bk, '||', masat_supplier_purchasers.landcode) AS pk_supplier_buyer_country_code
    FROM {{ ref("rv_hub__supplier") }} AS hub_supplier
    INNER JOIN masat_supplier_purchasers
        ON hub_supplier.supplier_hk = masat_supplier_purchasers.supplier_hk
)

SELECT
    pk_supplier_buyer_country_code AS pk_supplier_buyer_code,
    fk_supplier_code,
    fk_employee_code,
    buyer,
    country,
    headbuyer
FROM final
