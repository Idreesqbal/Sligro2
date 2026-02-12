WITH input AS (
    SELECT
        CAST('2024-01-01 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        true AS _fivetran_deleted,
        1 AS row_id
    UNION ALL
    SELECT
        CAST('2024-01-02 00:00:00' AS TIMESTAMP) AS _fivetran_synced,
        false AS _fivetran_deleted,
        2 AS row_id

)

SELECT
    {{ fivetran_enddate()}} AS output,
    *
FROM input
