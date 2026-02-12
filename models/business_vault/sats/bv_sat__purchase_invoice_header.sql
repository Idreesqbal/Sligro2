WITH ranked AS (
    SELECT
        purchase_invoice_hk,
        hashdiff,
        IF(
            bidtif > 0,
            PARSE_DATE('%y%m%d', LPAD(CAST(bidtif AS STRING), 6, '0')),
            null
        ) AS bidtif,
        btkdif,
        bvdtif,
        fgdtif,
        fkaoif,
        fkbtif,
        fkdtif,
        fknrif,
        grbaif,
        grbbif,
        hfdkif,
        IF(
            ifdtif > 0,
            PARSE_DATE('%y%m%d', LPAD(CAST(ifdtif AS STRING), 6, '0')),
            null
        ) AS ifdtif,
        ifusif,
        i2dtif,
        IF(
            jfdtif > 0,
            PARSE_DATE('%y%m%d', LPAD(CAST(jfdtif AS STRING), 6, '0')),
            null
        ) AS jfdtif,
        IF(
            jidtif > 0,
            PARSE_DATE('%y%m%d', LPAD(CAST(jidtif AS STRING), 6, '0')),
            null
        ) AS jidtif,
        jvdtif,
        ogdtif,
        ovdtif,
        stkdif,
        stsuif,
        toifif,
        vfdtif,
        vvdtif,
        vvusif,
        vakdif,
        start_datetime,
        end_datetime,
        load_datetime,
        record_source,

        ROW_NUMBER() OVER (
            PARTITION BY purchase_invoice_hk
            ORDER BY end_datetime DESC
        ) AS rn
    FROM {{ ref('rv_sat__purchase_invoice') }}
)

SELECT
    purchase_invoice_hk,
    hashdiff,
    bidtif,
    btkdif,
    bvdtif,
    fgdtif,
    fkaoif,
    fkbtif,
    fkdtif,
    fknrif,
    grbaif,
    grbbif,
    hfdkif,
    ifdtif,
    ifusif,
    i2dtif,
    jfdtif,
    jidtif,
    jvdtif,
    ogdtif,
    ovdtif,
    stkdif,
    stsuif,
    toifif,
    vfdtif,
    vvdtif,
    vvusif,
    vakdif,
    start_datetime,
    end_datetime,
    load_datetime,
    record_source
FROM ranked
WHERE rn = 1
