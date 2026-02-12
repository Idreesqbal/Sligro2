{%- set yaml_metadata -%}
source_model: "bv_bdv__forecast__forecast"
src_pk:
    - "purchase_forecast_hk"
    - "order_date"
src_ldts: "load_datetime"
src_payload:
    - "purchase_forecast_hk"
    - "order_date"
    - "purchase_forecast"
    - "delivery_date"
    - "record_source"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ scd2_for_subset_of_columns(**metadata_dict) }}
