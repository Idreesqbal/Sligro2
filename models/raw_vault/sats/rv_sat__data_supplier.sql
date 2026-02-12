{{ config(
    materialized='incremental',
    unique_key= ['RPE_SUPPLIER_HK', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_article_data_supplier"
src_pk:
    - "RPE_SUPPLIER_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "supplier_id"
    - "supplier_name"
    - "supplier_gln"
src_ldts: "LOAD_DATETIME"
src_extra_columns:
    - "END_DATETIME"
    - "RPE_SUPPLIER_BK"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
