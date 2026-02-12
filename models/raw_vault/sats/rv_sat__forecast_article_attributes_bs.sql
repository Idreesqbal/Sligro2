{{
  config(
    unique_key = ['FORECAST_HK','LOAD_DATETIME'],
    )
}}

{%- set yaml_metadata -%}
source_model: "stg_slim4__bs_article_attributes"
src_pk: "FORECAST_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "is_stock_controlled"
    - "trend"
    - "season_status"
    - "seasonal_probability"
    - "season"
    - "season_profile"
    - "use_aggregated_client_forecast"
    - "relevant_period"
    - "tracking_signal"
    - "offset"
    - "forecast_exception"
    - "stock_exception"
    - "sales_class"
    - "base_forecast"
    - "event_forecast"
    - "confirmed_extra"
    - "total_safety_stock"
    - "order_level"
    - "purchase_advice"
    - "maximum_loss"
    - "resulting_loss_service_level"
    - "service_level"
    - "sales_class_short_description"
    - "sales_class_long_description"
    - "forecast_exception_short_description"
    - "forecast_exception_long_description"
    - "stock_exception_short_description"
    - "stock_exception_long_description"
    - "START_DATETIME"
src_ldts: "LOAD_DATETIME"
src_extra_columns:
    - "END_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
