{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__postcode4'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__Info_Postcode4"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    POSTCODE_BK: "CAST(sizopcd4 AS STRING)"
    SAT_POSTCODE: "!rv_sat__postcode_details"
hashed_columns:
    POSTCODE_HK:
        - "POSTCODE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "sizopcd4"
            - "sizopcd3"
            - "sizopcd2"
            - "sizopcd1"
            - "sizcgmc"
            - "sizcprov"
            - "sizwprov"
            - "sizoplt"
            - "sizopltu"
            - "sizlpbs"
            - "sizcvakreg"
            - "sizcvregio"
            - "sizolatit"
            - "sizolongit"
            - "sizogeo"
            - "sizogeotxt"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
