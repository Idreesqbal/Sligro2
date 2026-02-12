{%- set yaml_metadata -%}
source_model:
    base_stg_slim4__bs_forecast
derived_columns:
    RECORD_SOURCE: "!slim4__bs_forecast"
    LOAD_DATETIME: TIMESTAMP(dt)
    START_DATETIME: TIMESTAMP(dt)
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    FORECAST_BK: "slim4_id"
    ARTICLE_BK: "CAST(CAST(legacy_article_number AS INTEGER) AS STRING)"
    CUSTOMER_BK: "CAST(SPLIT(slim4_id, '-')[SAFE_OFFSET(1)] AS STRING)"
    SITE_BK: "CAST(CAST(store_number AS INTEGER) AS STRING)"
    SUPPLIER_BK: "CAST(CAST(supplier_number AS INTEGER) AS STRING)"
    DATE: "SAFE.PARSE_DATE('%Y%m%d',date)"
    DELIVERY_DATE: "SAFE.PARSE_DATE('%Y%m%d',delivery_date)"
    ORDER_DATE: "SAFE.PARSE_DATE('%Y%m%d',order_date)"
hashed_columns:
    FORECAST_HK: "FORECAST_BK"
    ARTICLE_HK: "ARTICLE_BK"
    CUSTOMER_HK: "CUSTOMER_BK"
    SITE_HK: "SITE_BK"
    SUPPLIER_HK: "SUPPLIER_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "slim4_id"
            - "legacy_article_number"
            - "store_number"
            - "supplier_number"
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
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
