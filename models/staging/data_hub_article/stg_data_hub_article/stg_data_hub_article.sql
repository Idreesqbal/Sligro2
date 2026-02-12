{%- set yaml_metadata -%}
source_model:
    'base_data_hub_article'
hashed_columns:
    ARTICLE_HK:
        - "ARTICLE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "articleCode"
            - "sligroLegacyBrandCode"
            - "brandName"
            - "scalesArticle"
            - "pluCode"
            - "variableWeightItem"
            - "orderunitofmeasure"
            - "baseunitofmeasure"
            - "articleType"
            - "issueunitofmeasure"
            - "is_created"
            - "is_deleted"
derived_columns:
    RECORD_SOURCE: "!Data_Hub_Article"
    LOAD_DATETIME: "event_datetime"
    END_DATETIME: CAST(NULL AS TIMESTAMP)
    ARTICLE_BK: CAST(articleCode AS STRING)
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
