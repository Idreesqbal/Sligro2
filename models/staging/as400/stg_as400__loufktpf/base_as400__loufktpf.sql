--Created a base to add the calculation on datum befre the BK is created in the source
WITH source AS (
    SELECT
        faktnr,
        filnr,
        fakris,
        klanr,
        betkod,
        buisnr,
        embsal,
        inbkh,
        jnkdlt,
        minuut,
        netexc,
        netto,
        pcnr,
        specpr,
        srtfak,
        uur,
        vrzfak,
        _fivetran_deleted,
        _fivetran_synced,
        _fivetran_active,
        _fivetran_start,
        _fivetran_end,
        CAST(LPAD(CAST(datum AS STRING), 8, '20') AS INT) AS factuurdatum,
        CAST(REPLACE(LPAD(CAST(vrzdat AS STRING), 8, '20'), '20202020', '20201231') AS INT) AS vrzdat
    FROM
        {{ source('sligro60_slglouwers_slgfilelib_slgfilelib', 'loufktpf') }}

)

SELECT *
FROM source
WHERE _fivetran_synced > '2024-12-16 10:00:00.000000'
