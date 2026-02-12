{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__postcode4_afstanden'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__Info_Postcode4_Afstanden"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    SIZOPCD4VA: "CAST(SIZOPCD4VA AS INT64)"
    SIZOPCD4NA: "CAST(SIZOPCD4NA AS INT64)"
    POSTCODE_FROM_BK: "CAST(SIZOPCD4VA AS INT64)"
    POSTCODE_TO_BK: "CAST(SIZOPCD4NA AS INT64)"
    POSTCODE_FROM_TO_BK:
        - "sizopcd4va"
        - "sizopcd4na"
    SAT_DISTANCE: "!rv_sat__postcode_distance"
hashed_columns:
    POSTCODE_FROM_HK:
        - "POSTCODE_FROM_BK"
    POSTCODE_TO_HK:
        - "POSTCODE_TO_BK"
    POSTCODE_FROM_TO_HK:
        - "POSTCODE_FROM_TO_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "sizopcd4va"
            - "sizopcd4na"
            - "sizwafst"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
