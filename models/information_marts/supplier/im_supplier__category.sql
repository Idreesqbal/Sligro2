WITH
masat_supplier_category AS (
    SELECT
        supplier_hk,
        categorie
    FROM {{ ref("rv_masat__supplier_category") }}
    WHERE end_datetime IS null
),

final AS (
    SELECT
        hub_supplier.supplier_bk AS fk_supplier_code,
        masat_supplier_category.categorie AS category,
        {# a supplier can have multiple categories.
        Because of multiple categories only the supplier_bk is not sufficient.
        Therefore the addition of the category #}
        CONCAT(hub_supplier.supplier_bk, '||', masat_supplier_category.categorie) AS pk_supplier_category_code
    FROM {{ ref("rv_hub__supplier") }} AS hub_supplier
    INNER JOIN masat_supplier_category
        ON hub_supplier.supplier_hk = masat_supplier_category.supplier_hk
)

SELECT
    pk_supplier_category_code,
    fk_supplier_code,
    category AS supplier_category
FROM final
