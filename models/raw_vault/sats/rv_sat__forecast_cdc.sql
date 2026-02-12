{{
  config(
    unique_key = ['FORECAST_HK', 'date', 'LOAD_DATETIME'],
    )
}}

{%- set yaml_metadata -%}
source_model: "stg_slim4__cdc_forecast"
src_pk:
    - "FORECAST_HK"
    - "date"
src_hashdiff: "HASHDIFF"
src_payload:
    - "week_number"
    - "day_number"
    - "date"
    - "starting_stock"
    - "forecast"
    - "event_forecast"
    - "total_confirmed_demand"
    - "additional_above_demand"
    - "on_order"
    - "to_order"
    - "purchase_forecast"
    - "end_stock"
    - "delivery_date"
    - "order_date"
    - "supplier_name"
    - "CUSTOMER_BK"
    - "SITE_BK"
    - "ARTICLE_BK"
    - "START_DATETIME"
src_ldts: "LOAD_DATETIME"
src_extra_columns:
    - "END_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
