{{ config(
    materialized='incremental',
    unique_key= ['ARTICLE_HK', 'locale', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_data_hub_article_type_specific_attribute"
src_pk:
    - "ARTICLE_HK"
    - "locale"
src_hashdiff: "HASHDIFF"
src_payload:
    - "name"
    - "value"
    - "is_deleted"
    - "is_created"
src_ldts: "LOAD_DATETIME"
src_extra_columns:
    - "END_DATETIME"
    - "is_created"
    - "is_deleted"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
