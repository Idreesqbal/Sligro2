{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: filia2pf
derived_columns:
    RECORD_SOURCE: "!AS400_filia2pf"
    LOAD_DATETIME: "{{ fivetran_loaddate(['finrc1']) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    SITE_BK: "TRIM(CAST(finrc1 AS STRING))"
    SALES_FORMULA_BK: "TRIM(CAST(frmlc1 AS STRING))"
    frmlc1:
        - "NULLIF(TRIM(frmlc1), '')"
    synmc1:
        - "NULLIF(TRIM(synmc1), '')"
    todrc1:
        - "NULLIF(TRIM(todrc1), '')"
    vsdac1:
        - "NULLIF(TRIM(vsdac1), '')"
    vsdbc1:
        - "NULLIF(TRIM(vsdbc1), '')"
    vssac1:
        - "NULLIF(TRIM(vssac1), '')"
    vssbc1:
        - "NULLIF(TRIM(vssbc1), '')"
    wzdrc1:
        - "NULLIF(TRIM(wzdrc1), '')"
    losoc1:
        - "NULLIF(TRIM(CAST(losoc1 AS STRING)), '')"

hashed_columns:
    SITE_HK:
        - "SITE_BK"
    SALES_FORMULA_HK:
        - "SALES_FORMULA_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
        - "dtdac1"
        - "dtdbc1"
        - "dtsac1"
        - "dtsbc1"
        - "finrc1"
        - "frmlc1"
        - "losoc1"
        - "s006c1"
        - "synmc1"
        - "todrc1"
        - "todtc1"
        - "vsdac1"
        - "vsdbc1"
        - "vssac1"
        - "vssbc1"
        - "wzdrc1"
        - "wzdtc1"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
