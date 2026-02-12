{{ config(
    materialized='incremental',
    unique_key= ['COMPANY_REFERENCE_LOCATION_HK', 'POSTCODE6', 'LOAD_DATETIME']
) }}
{%- set yaml_metadata -%}
source_model: "stg_company_info__locaties"
src_pk:
    - "COMPANY_REFERENCE_LOCATION_HK"
    - "POSTCODE6"
src_hashdiff: "HASHDIFF"
src_payload:
    - "sizolockey"
    - "sizokey_up"
    - "sizosbil"
    - "sizosbi2l"
    - "sizosbisel"
    - "sizwam_loc"
    - "sizkam_loc"
    - "sizwai_loc"
    - "sizwlftl"
    - "sizklftl"
    - "sizwasbi2l"
    - "sizpmarbrl"
    - "sizkmarbrl"
    - "sizpclobal"
    - "sizkclobal"
    - "sizlondwel"
    - "sizlcustrl"
    - "gw_kwhitel"
    - "gw_kbluel"
    - "gw_kmobtll"
    - "gw_kjvgasl"
    - "gw_kjvelel"
    - "sellipl"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
