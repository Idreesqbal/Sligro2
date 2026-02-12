WITH input AS (
    SELECT
        'primary_key_1' AS primary_key,
        CAST('2024-01-01 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        true AS _fivetran_deleted,
        1 AS row_id
    UNION ALL
    SELECT
        'primary_key_1' AS primary_key,
        CAST('2024-01-01 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        false AS _fivetran_deleted,
        2 AS row_id
    UNION ALL
    SELECT
        'primary_key_2' AS primary_key,
        CAST('2024-01-02 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        true AS _fivetran_deleted,
        3 AS row_id
    UNION ALL
    SELECT
        'primary_key_2' AS primary_key,
        CAST('2024-01-03 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        false AS _fivetran_deleted,
        4 AS row_id
    UNION ALL
    SELECT
        'primary_key_2' AS primary_key,
        CAST('2024-01-03 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        true AS _fivetran_deleted,
        5 AS row_id
)

SELECT
    {{ fivetran_loaddate(['primary_key']) }} AS output,
    *
FROM input
