{%- set yaml_metadata -%}
source_model:
    sligro60_slgmutlib_slgfilelib: dcnawpf
derived_columns:
    RECORD_SOURCE: "!AS400_dcnawpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SITE_BK: "TRIM(CAST(finrdn AS STRING))"
    adrsdn:
        - "NULLIF(TRIM(adrsdn), '')"
    dckddn:
        - "NULLIF(TRIM(dckddn), '')"
    dcomdn:
        - "NULLIF(TRIM(dcomdn), '')"
    dcpkdn:
        - "NULLIF(TRIM(dcpkdn), '')"
    dctadn:
        - "NULLIF(TRIM(dctadn), '')"
    dcwpdn:
        - "NULLIF(TRIM(dcwpdn), '')"
    fxnrdn:
        - "NULLIF(TRIM(fxnrdn), '')"

hashed_columns:
    SITE_HK:
        - "SITE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "adrsdn"
            - "dckddn"
            - "dcomdn"
            - "dcpkdn"
            - "dcstdn"
            - "dctadn"
            - "dcwpdn"
            - "finrdn"
            - "fxnrdn"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
