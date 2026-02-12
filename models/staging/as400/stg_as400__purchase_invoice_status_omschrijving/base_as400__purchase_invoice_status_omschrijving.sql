WITH source AS (
    SELECT *
    FROM
        {{ source('sligro60_slgfilelib_slgfilelib', 'status_omschrijving') }}

)

SELECT *
FROM source
