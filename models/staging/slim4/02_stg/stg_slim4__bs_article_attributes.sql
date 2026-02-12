{%- set yaml_metadata -%}
source_model:
    base_stg_slim4__bs_article_attributes
derived_columns:
    RECORD_SOURCE: "!slim4__bs_article_attributes"
    LOAD_DATETIME: TIMESTAMP(dt)
    START_DATETIME: TIMESTAMP(dt)
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    ARTICLE_BK: "CAST(CAST(legacy_article_number AS INTEGER) AS STRING)"
    CUSTOMER_BK: "CAST(COALESCE(SPLIT(slim4_id, '-')[SAFE_OFFSET(1)], '') AS STRING)"
    SITE_BK: "CAST(CAST(store_number AS INTEGER) AS STRING)"
    FORECAST_BK: "slim4_id"
    USE_AGGREGATED_CLIENT_FORECAST: "CAST(use_aggregated_client_forecast AS BOOL)"
    IS_STOCK_CONTROLLED: "CAST(is_stock_controlled AS BOOL)"
hashed_columns:
    ARTICLE_HK: "ARTICLE_BK"
    CUSTOMER_HK: "CUSTOMER_BK"
    SITE_HK: "SITE_BK"
    FORECAST_HK : "FORECAST_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "slim4_id"
            - "legacy_article_number"
            - "store_number"
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
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
