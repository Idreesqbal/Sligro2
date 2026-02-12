{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: filialpf
derived_columns:
    RECORD_SOURCE: "!AS400_filialpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SITE_BK: "TRIM(CAST(finrc1 AS STRING))"
    HEADOFFICE_BK: "TRIM(CAST(finrc4 AS STRING))"
    adrsc1:
        - "NULLIF(TRIM(adrsc1), '')"
    fisrc1:
        - "NULLIF(TRIM(fisrc1), '')"
    fxnrc1:
        - "NULLIF(TRIM(fxnrc1), '')"
    ifjnc1:
        - "NULLIF(TRIM(ifjnc1), '')"
    naamc1:
        - "NULLIF(TRIM(naamc1), '')"
    nfm1c1:
        - "NULLIF(TRIM(nfm1c1), '')"
    nfm2c1:
        - "NULLIF(TRIM(nfm2c1), '')"
    nfm3c1:
        - "NULLIF(TRIM(nfm3c1), '')"
    nft1c1:
        - "NULLIF(TRIM(nft1c1), '')"
    pskdc1:
        - "NULLIF(TRIM(pskdc1), '')"
    retvc1:
        - "NULLIF(TRIM(retvc1), '')"
    rpjnc1:
        - "NULLIF(TRIM(rpjnc1), '')"
    sejnc1:
        - "NULLIF(TRIM(sejnc1), '')"
    srk1c1:
        - "NULLIF(TRIM(srk1c1), '')"
    srk2c1:
        - "NULLIF(TRIM(srk2c1), '')"
    srk3c1:
        - "NULLIF(TRIM(srk3c1), '')"
    srk4c1:
        - "NULLIF(TRIM(srk4c1), '')"
    srk5c1:
        - "NULLIF(TRIM(CAST(srk5c1 AS STRING)), '')"
    tfnrc1:
        - "NULLIF(TRIM(tfnrc1), '')"
    tiosc1:
        - "NULLIF(TRIM(tiosc1), '')"
    vn04c1:
        - "NULLIF(TRIM(vn04c1), '')"
    vn10c1:
        - "NULLIF(TRIM(vn10c1), '')"
    wnplc1:
        - "NULLIF(TRIM(wnplc1), '')"
    zmkdc1:
        - "NULLIF(TRIM(zmkdc1), '')"
    von5c1:
        - "NULLIF(TRIM(CAST(von5c1 AS STRING)), '')"

hashed_columns:
    SITE_HK:
        - "SITE_BK"
    SITE_HEADOFFICE_HK:
        - "SITE_BK"
        - "HEADOFFICE_BK"
    HEADOFFICE_HK:
        "HEADOFFICE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "adrsc1"
            - "fin1c1"
            - "finrc1"
            - "finrc4"
            - "finsc1"
            - "fisrc1"
            - "fltyc1"
            - "fxnrc1"
            - "ifjnc1"
            - "naamc1"
            - "nfm1c1"
            - "nfm2c1"
            - "nfm3c1"
            - "nft1c1"
            - "pfnrc1"
            - "primc1"
            - "pritc1"
            - "pskdc1"
            - "retvc1"
            - "rpjnc1"
            - "s006c1"
            - "sejnc1"
            - "srk1c1"
            - "srk2c1"
            - "srk3c1"
            - "srk4c1"
            - "srk5c1"
            - "tfnrc1"
            - "tiosc1"
            - "vn04c1"
            - "vn10c1"
            - "von1c1"
            - "von2c1"
            - "von3c1"
            - "von4c1"
            - "von5c1"
            - "wnplc1"
            - "zmkdc1"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
