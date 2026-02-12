WITH input AS (
    SELECT
        'X' AS key_a,
        '1' AS key_b,
        CAST('2024-01-01 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        true AS _fivetran_deleted,
        1 AS row_id
    UNION ALL
    SELECT
        'X' AS key_a,
        '1' AS key_b,
        CAST('2024-01-01 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        false AS _fivetran_deleted,
        2 AS row_id
    UNION ALL
    SELECT
        'Y' AS key_a,
        '2' AS key_b,
        CAST('2024-01-02 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        true AS _fivetran_deleted,
        3 AS row_id
    UNION ALL
    SELECT
        'Y' AS key_a,
        '2' AS key_b,
        CAST('2024-01-03 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        false AS _fivetran_deleted,
        4 AS row_id
    UNION ALL
    SELECT
        'Y' AS key_a,
        '2' AS key_b,
        CAST('2024-01-03 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        true AS _fivetran_deleted,
        5 AS row_id
)

SELECT
    {{ fivetran_loaddate(['key_a', 'key_b']) }} AS output,
    *
FROM input
