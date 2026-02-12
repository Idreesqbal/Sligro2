-- This model was switched to history mode on 2025-01-15. We ignore all data from before.

WITH source AS (
    SELECT *
    FROM {{ source('sligro60_slgmutlib_slgfilelib', 'artikcpf') }}
)

SELECT *
FROM source
WHERE _fivetran_synced > '2025-01-15 12:00:00.000000'
