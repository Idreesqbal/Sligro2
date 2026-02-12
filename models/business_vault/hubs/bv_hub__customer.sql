{{
    config(
        materialized='incremental'
    )
}}

{%- set yaml_metadata -%}
source_model:
    - "bv_bdv__customer_customer"
src_pk: "CUSTOMER_HK"
src_nk: "CUSTOMER_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(**metadata_dict) }}
