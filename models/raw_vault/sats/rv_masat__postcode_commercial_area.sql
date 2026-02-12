{{ config(
    materialized='incremental',
    unique_key= ['POSTCODE_HK', 'sizopcd', 'sizobtrvnr','LOAD_DATETIME']
) }}
{%- set yaml_metadata -%}
source_model: "stg_company_info__bedrijfsterreinen"
src_pk:
    - "POSTCODE_HK"
    - "sizopcd"
    - "sizobtrvnr"
src_hashdiff: "HASHDIFF"
src_payload:
    - "sizopcd"
    - "sizoplannm"
    - "sizokernnm"
    - "sizobtrvnr"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
