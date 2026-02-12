{{
  config(
    materialized = 'incremental',
    unique_key = ['CUSTOMER_HK', 'CUSTOMER_HIERARCHY_HK','LOAD_DATETIME'],
    apply_source_filter = true
    )
}}

{%- set yaml_metadata -%}
source_model: "stg_data_hub_customer_hierarchy"
src_pk:
  - "CUSTOMER_HIERARCHY_HK"
  - "CUSTOMER_HK"
  - "CUSTOMER_PARENT_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "customerId"
    - "registrationDate"
    - "hierarchyType"
    - "parentCustomerId"
    - "is_deleted"
    - "is_created"
src_ldts: "LOAD_DATETIME"
src_extra_columns:
    - "END_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
