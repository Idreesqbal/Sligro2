WITH purchase_order_header AS (
    SELECT *
    FROM
        {{ source('sligro60_filchg_slgfilelib', 'bstalgpf') }}
    WHERE
        _fivetran_synced > '2024-12-16 10:00:00.000000'
        {# This is applied based on a BI request to cut-off the data
        from when we switched to history mode in fivetran.#}
        AND {{ cast_to_date('iodtba') }} >= '2024-12-16'
),

distribution_center AS (
    SELECT *
    FROM
        {{ source('sligro60_slgmutlib_slgfilelib', 'dcnawpf') }}
-- WHERE _fivetran_synced > '2024-12-16 10:00:00.000000'
),

status_description AS (
    SELECT
        TRIM(lgomot) AS stkdba_description,
        CAST(SUBSTR(TRIM(wakdot), 8, 2) AS int64) AS stkdba
    FROM
        {{ source('sligro60_slgmutlib_slgfilelib', 'omstabpf') }}
    WHERE _fivetran_deleted = false AND TRIM(omkdot) = 'BSTALG' AND TRIM(wakdot) LIKE 'STKDBA%'
),

type_description AS (
    SELECT
        TRIM(lgomot) AS iosoba_description,
        SUBSTR(TRIM(wakdot), 8, 1) AS iosoba
    FROM
        {{ source('sligro60_slgmutlib_slgfilelib', 'omstabpf') }}
    WHERE _fivetran_deleted = false AND TRIM(omkdot) = 'BSTALG' AND TRIM(wakdot) LIKE 'IOSOBA%'
),

communication_method_description AS (
    SELECT
        TRIM(lgomot) AS znkdba_description,
        SUBSTR(TRIM(wakdot), 8, 1) AS znkdba
    FROM
        {{ source('sligro60_slgmutlib_slgfilelib', 'omstabpf') }}
    WHERE _fivetran_deleted = false AND TRIM(omkdot) = 'BSTALG' AND TRIM(wakdot) LIKE 'ZNKDBA%'
)

SELECT
    purchase_order_header.*,
    distribution_center.finrdn AS look_up_site_code,
    status_description.stkdba_description,
    type_description.iosoba_description,
    communication_method_description.znkdba_description
FROM purchase_order_header
-- Join on the distribution center to get the look up values of dckdba.
LEFT JOIN distribution_center ON TRIM(purchase_order_header.dckdba) = TRIM(distribution_center.dckddn)
LEFT JOIN status_description ON CAST(purchase_order_header.stkdba AS int64) = status_description.stkdba
LEFT JOIN type_description ON TRIM(purchase_order_header.iosoba) = type_description.iosoba
LEFT JOIN
    communication_method_description
    ON TRIM(purchase_order_header.znkdba) = communication_method_description.znkdba
