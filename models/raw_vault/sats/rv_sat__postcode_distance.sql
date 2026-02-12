{{ config(
    materialized='incremental',
    unique_key= ['POSTCODE_FROM_TO_HK', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_company_info__postcode4_afstanden"
src_pk: "POSTCODE_FROM_TO_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "SIZOPCD4VA"
    - "SIZOPCD4NA"
    - "SIZWAFST"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
