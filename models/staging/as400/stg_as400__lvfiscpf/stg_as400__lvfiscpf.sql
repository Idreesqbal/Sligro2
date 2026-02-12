{%- set yaml_metadata -%}
source_model:
    'base_as400__lvfiscpf'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__lvfiscpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SUPPLIER_BK:
        - "TRIM(CAST(lvnrc1 AS STRING))"
    {# Technical changes to source columns #}
    aardc1:
        - "TRIM(CAST(aardc1 AS STRING))"
    akrtc1:
        - "TRIM(CAST(akrtc1 AS STRING))"
    bclrc1:
        - "TRIM(CAST(bclrc1 AS STRING))"
    betwc1:
        - "TRIM(CAST(betwc1 AS STRING))"
    bibuc1:
        - "TRIM(CAST(bibuc1 AS STRING))"
    blndc1:
        - "TRIM(CAST(blndc1 AS STRING))"
    bspic1:
        - "TRIM(CAST(bspic1 AS STRING))"
    grpac1:
        - "TRIM(CAST(grpac1 AS STRING))"
    ibanc1:
        - "TRIM(CAST(ibanc1 AS STRING))"
    kab3c1:
        - "TRIM(CAST(kab3c1 AS STRING))"
    kbpoc1:
        - "TRIM(CAST(kbpoc1 AS STRING))"
    krtcc1:
        - "TRIM(CAST(krtcc1 AS STRING))"
    kstcc1:
        - "TRIM(CAST(kstcc1 AS STRING))"
    landc1:
        - "TRIM(CAST(landc1 AS STRING))"
    lskdc1:
        - "TRIM(CAST(lskdc1 AS STRING))"
    nmplc1:
        - "TRIM(CAST(nmplc1 AS STRING))"
    nwb1c1:
        - "TRIM(CAST(nwb1c1 AS STRING))"
    nwb2c1:
        - "TRIM(CAST(nwb2c1 AS STRING))"
    nwb3c1:
        - "TRIM(CAST(nwb3c1 AS STRING))"
    nwb4c1:
        - "TRIM(CAST(nwb4c1 AS STRING))"
    oms3c1:
        - "TRIM(CAST(oms3c1 AS STRING))"
    pltsc1:
        - "TRIM(CAST(pltsc1 AS STRING))"
    pstkc1:
        - "TRIM(CAST(pstkc1 AS STRING))"
    reknc2:
        - "TRIM(CAST(reknc2 AS STRING))"
    rnrac1:
        - "TRIM(CAST(rnrac1 AS STRING))"
    selkc1:
        - "TRIM(CAST(selkc1 AS STRING))"
    swftc1:
        - "TRIM(CAST(swftc1 AS STRING))"

hashed_columns:
    SUPPLIER_HK:
        - "SUPPLIER_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "lvnrc1"
            - "AARDC1"
            - "AKRTC1"
            - "BCLRC1"
            - "BETWC1"
            - "BIBUC1"
            - "BIDAC1"
            - "BLNDC1"
            - "BSPIC1"
            - "EANCC1"
            - "EXT1C1"
            - "EXT2C1"
            - "EXT3C1"
            - "GRPAC1"
            - "IBANC1"
            - "KAB3C1"
            - "KBPOC1"
            - "KRTCC1"
            - "KSTCC1"
            - "LANDC1"
            - "LSKDC1"
            - "LVSUC1"
            - "NMPLC1"
            - "NWB1C1"
            - "NWB2C1"
            - "NWB3C1"
            - "NWB4C1"
            - "OMS3C1"
            - "PLTSC1"
            - "PSPKC1"
            - "PSTKC1"
            - "REKNC2"
            - "RNRAC1"
            - "RWPLC1"
            - "SELKC1"
            - "SWFTC1"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
