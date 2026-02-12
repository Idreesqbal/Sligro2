{{
    config(
        materialized='incremental'
    )
}}

{%- set yaml_metadata -%}
source_model:
    - "bv_bdv__company_reference_customer_matching"
src_pk: "COMPANY_REFERENCE_CUSTOMER_HK"
src_fk:
    - "COMPANY_REFERENCE_HK"
    - "CUSTOMER_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
