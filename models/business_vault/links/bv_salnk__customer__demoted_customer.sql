{{
    config(
        materialized='incremental'
    )
}}

{%- set yaml_metadata -%}
source_model:
    - "bv_bdv__customer_6_to_7_digits"
src_pk: "customer__demoted_customer_hk"
src_fk:
    - "customer_hk"
    - "demoted_customer_hk"

src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
