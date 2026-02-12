{{ config(
    materialized='incremental',
    unique_key= ['TRADE_ITEM_HK', 'additional_gtin', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_data_hub_trade_item_additional_gtin"
src_pk:
    - "TRADE_ITEM_HK"
    - "additional_gtin"
src_hashdiff: "HASHDIFF"
src_payload:
    - "main_gtin"
    - "is_created"
    - "is_deleted"
src_ldts: "LOAD_DATETIME"
src_extra_columns:
    - "END_DATETIME"
    - "is_created"
    - "is_deleted"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
