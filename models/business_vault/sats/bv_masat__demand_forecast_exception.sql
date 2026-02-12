{{
  config(
    unique_key = ['pk_exception_code', 'LOAD_DATETIME'],
    )
}}

{%- set yaml_metadata -%}
source_model: "bv_bdv__forecast__exception"
src_pk: "pk_exception_code"
src_ldts: "load_datetime"
src_payload:
    - "pk_exception_code"
    - "demand_forecast_hk"
    - "exception_type"
    - "exception_code"
    - "short_description"
    - "long_description"
    - "record_source"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ scd2_for_subset_of_columns(**metadata_dict) }} --noqa
