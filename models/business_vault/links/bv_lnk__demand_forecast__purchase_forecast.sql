
{%- set yaml_metadata -%}
source_model: "bv_bdv__forecast__forecast"
src_pk: "DEMAND_FORECAST_PURCHASE_FORECAST_HK"
src_fk:
    - "DEMAND_FORECAST_HK"
    - "PURCHASE_FORECAST_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
