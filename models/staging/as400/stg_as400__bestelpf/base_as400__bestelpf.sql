WITH source AS (
    SELECT *
    FROM {{ source('sligro60_filchg_slgfilelib', 'bestelpf') }}
    WHERE _fivetran_synced > '2024-12-16 10:00:00.000000'
),

vat_rate_description AS (
    SELECT
        cast(trim(lgomot) AS bignumeric) AS btkdbe_description,
        substr(trim(wakdot), 8, 1) AS btkdbe
    FROM
        {{ source('sligro60_slgmutlib_slgfilelib', 'omstabpf') }}
    WHERE _fivetran_deleted = false AND trim(omkdot) = 'BESTELPF' AND trim(wakdot) LIKE 'BTKDBE%'
)

SELECT
    source.*,
    vat_rate_description.btkdbe_description
FROM source
LEFT JOIN vat_rate_description ON trim(source.btkdbe) = vat_rate_description.btkdbe
