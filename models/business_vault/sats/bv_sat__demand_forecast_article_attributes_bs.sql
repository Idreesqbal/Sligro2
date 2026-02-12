{%- set yaml_metadata -%}
source_model: "bv_bdv__forecast__attributes_bs"
src_pk:
    - "demand_forecast_hk"
src_ldts: "load_datetime"
src_payload:
    - "demand_forecast_hk"
    - "trend"
    - "use_aggregated_client_forecast"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ scd2_for_subset_of_columns(**metadata_dict) }}
