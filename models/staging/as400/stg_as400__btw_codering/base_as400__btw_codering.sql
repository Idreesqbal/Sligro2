WITH source AS (
    SELECT *
    FROM
        {{ source('sligro60_slgmutlib_centraal', 'btw_codering') }}
)

SELECT *
FROM
    source
