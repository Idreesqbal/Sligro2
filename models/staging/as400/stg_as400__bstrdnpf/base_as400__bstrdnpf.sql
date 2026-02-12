WITH source AS (
    SELECT *
    FROM
        {{ source('sligro60_slgfilelib_slgfilelib', 'bstrdnpf') }}

)

SELECT *
FROM
    source
