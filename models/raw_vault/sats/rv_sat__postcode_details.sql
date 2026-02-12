{{ config(
    materialized='incremental',
    unique_key= ['POSTCODE_HK', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_company_info__postcode4"
src_pk: "POSTCODE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
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
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
