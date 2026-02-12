WITH source AS (
    SELECT *
    FROM
        {{ source('sligro60_db260_centraal', 'prijs_groep') }}

)

SELECT *
FROM source
WHERE _fivetran_synced > '2024-12-16 10:00:00.000000'
