{{ config(
    materialized='incremental',
    unique_key= ['HASHDIFF', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_article_data_supplier"
src_pk:
    - "article_id"
src_hashdiff: "HASHDIFF"
src_payload:
    - "article_id"
    - "article_ownership_nl"
    - "article_ownership_be"
    - "unit"
src_ldts: "LOAD_DATETIME"
src_extra_columns:
    - "END_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
