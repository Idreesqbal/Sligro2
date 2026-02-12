{%- set yaml_metadata -%}
source_model:
    'base_data_hub_trade_item_sales_area'
derived_columns:
    RECORD_SOURCE: "!Data_Hub_Article"
    LOAD_DATETIME: "event_datetime"
    END_DATETIME: CAST(NULL AS TIMESTAMP)
    TRADE_ITEM_BK: CAST(gtin as STRING)
hashed_columns:
    TRADE_ITEM_HK:
        - "TRADE_ITEM_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "gtin"
            - "salesorganisationcode"
            - "salesunit"
            - "salesdistributionchannelcode"
            - "is_created"
            - "is_deleted"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
