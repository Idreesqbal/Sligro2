{{ config(
    materialized='incremental',
    unique_key= ['COMPANY_REFERENCE_HK', 'sizolockey', 'klantkey' ,'LOAD_DATETIME']
) }}
{%- set yaml_metadata -%}
source_model: "stg_company_info__keyword_categories"
src_pk:
    - "COMPANY_REFERENCE_HK"
    - "sizolockey"
    - "klantkey"
src_hashdiff: "HASHDIFF"
src_payload:
    - "sizokey"
    - "sizolockey"
    - "klantkey"
    - "afhalen"
    - "bezorgen"
    - "dark_kitchen"
    - "platformen"
    - "terras"
    - "uitsluiten"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
