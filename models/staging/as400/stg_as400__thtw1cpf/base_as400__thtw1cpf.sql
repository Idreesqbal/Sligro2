-- This model was switched to history mode on 2024-12-16. We ignore all data from before.

WITH source AS (
    SELECT *
    FROM {{ source('sligro60_slgfilelib_slgfilelib', 'thtw1cpf') }}
)

SELECT *
FROM source
WHERE _fivetran_synced > '2024-12-16 10:00:00.000000'
