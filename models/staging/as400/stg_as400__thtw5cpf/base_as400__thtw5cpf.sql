-- This is an Ugly Temporary Hack
-- There are issues with the source that we do not understand as of yet:
-- - Some stock has pallet number == 0, which should not be possible.
-- Pending the investigation into that, we simply keep that data out of here.
-- This model was switched to history mode on 2024-12-16. We ignore all data from before.


WITH source AS (
    SELECT *
    FROM {{ source('sligro60_slgfilelib_slgfilelib', 'thtw5cpf') }}
)

SELECT *
FROM source
WHERE panrc1 != 0 AND _fivetran_synced > '2024-12-16 10:00:00.000000'
-- There are duplicates with this start date
AND _fivetran_start != '2025-02-24 12:48:11.999000 UTC'
