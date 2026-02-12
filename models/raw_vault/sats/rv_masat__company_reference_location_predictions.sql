{{ config(
    materialized='incremental',
    unique_key= ['COMPANY_REFERENCE_LOCATION_HK', 'POSTCODE6', 'LOAD_DATETIME']
) }}
{%- set yaml_metadata -%}
source_model: "stg_company_info__voorspelmodellen_locatie"
src_pk:
    - "COMPANY_REFERENCE_LOCATION_HK"
    - "POSTCODE6"
src_hashdiff: "HASHDIFF"
src_payload:
    - "sizolockey"
    - "sizcbgt"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
