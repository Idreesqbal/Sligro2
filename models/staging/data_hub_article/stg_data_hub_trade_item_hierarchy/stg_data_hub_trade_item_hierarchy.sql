{%- set yaml_metadata -%}
source_model:
    'base_data_hub_trade_item_hierarchy'
derived_columns:
    RECORD_SOURCE: "!Data_Hub_Article"
    LOAD_DATETIME: "event_datetime"
    END_DATETIME: CAST(NULL AS TIMESTAMP)
    TRADE_ITEM_BK: CAST(gtin as STRING)
    TRADE_ITEM_CHILD_BK: CAST(childgtin as STRING)
hashed_columns:
    TRADE_ITEM_HK:
        - "TRADE_ITEM_BK"
    TRADE_ITEM_CHILD_HK:
        - "TRADE_ITEM_CHILD_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "gtin"
            - "quantityoflowerlevel"
            - "childgtin"
            - "is_created"
            - "is_deleted"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
