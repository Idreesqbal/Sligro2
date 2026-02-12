SELECT *
FROM {{ source('sligro60_slgmutlib_centraal', 'btw_codering_tarief') }}
--WHERE _fivetran_synced > '2024-12-16 10:00:00.000000'
