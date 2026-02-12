{%- set yaml_metadata -%}
source_model:
    'base_data_hub_trade_item'
derived_columns:
    RECORD_SOURCE: "!Data_Hub_Article"
    LOAD_DATETIME: "event_datetime"
    END_DATETIME: CAST(NULL AS TIMESTAMP)
    ARTICLE_BK: CAST(articleCode as STRING)
    TRADE_ITEM_BK: CAST(gtin as STRING)
    ARTICLE_TRADE_ITEM_BK:
        - "TRIM(CAST(articleCode as STRING))"
        - "TRIM(CAST(gtin as STRING))"
hashed_columns:
    ARTICLE_HK:
        - "ARTICLE_BK"
    TRADE_ITEM_HK:
        - "TRADE_ITEM_BK"
    ARTICLE_TRADE_ITEM_HK:
        - "ARTICLE_TRADE_ITEM_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "articleCode"
            - "unitOfMeasure"
            - "packagingWidthuom"
            - "packagingHeight"
            - "netWeightuom"
            - "netContentsuom"
            - "netWeight"
            - "netContents"
            - "packagingDepth"
            - "numberOfBaseUnits"
            - "drainedWeightuom"
            - "legacyArticleNumberSligroM"
            - "legacyArticleNumberSligro"
            - "grossWeight"
            - "packagingHeightuom"
            - "legacyArticleNumberAanTafel"
            - "packagingWidth"
            - "grossWeightuom"
            - "packagingDepthuom"
            - "drainedWeight"
            - "legacyArticleNumberSligroISPC"
            - "is_deleted"
            - "is_created"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
