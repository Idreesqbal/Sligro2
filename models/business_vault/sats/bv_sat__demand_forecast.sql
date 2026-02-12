{%- set yaml_metadata -%}
source_model: "bv_bdv__forecast__forecast"
src_pk:
    - "demand_forecast_hk"
    - "date"
src_ldts: "load_datetime"
src_payload:
    - "demand_forecast_hk"
    - "record_source"
    - "date"
    - "end_date"
    - "forecast"
    - "event_forecast"
    - "total_confirmed_demand"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ scd2_for_subset_of_columns(**metadata_dict) }}
