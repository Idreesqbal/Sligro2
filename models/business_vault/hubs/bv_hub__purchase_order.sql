{{
    config(
        materialized='incremental'
    )
}}

{%- set yaml_metadata -%}
source_model:
    - "bv_bdv__purchase_order_line"
src_pk: "PURCHASE_ORDER_HK"
src_nk: "PURCHASE_ORDER_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(**metadata_dict) }}
