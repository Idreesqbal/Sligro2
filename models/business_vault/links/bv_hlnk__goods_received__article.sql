{{
    config(
        materialized='incremental'
    )
}}

{%- set yaml_metadata -%}
source_model:
    - "bv_bdv__goods_received_line"

src_pk: "GOODS_RECEIVED_ARTICLE_HK"
src_fk:
    - "GOODS_RECEIVED_HK"
    - "ARTICLE_HK"

src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
