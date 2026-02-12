{%- set yaml_metadata -%}
source_model:
    - "stg_company_info__postcode4_afstanden"
src_pk: "POSTCODE_FROM_TO_HK"
src_fk:
    - "POSTCODE_FROM_HK"
    - "POSTCODE_TO_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
