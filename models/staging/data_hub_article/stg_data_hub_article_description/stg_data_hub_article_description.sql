{%- set yaml_metadata -%}
source_model:
    'base_data_hub_article_description'
derived_columns:
    RECORD_SOURCE: "!Data_Hub_Article"
    LOAD_DATETIME: "event_datetime"
    END_DATETIME: CAST(NULL AS TIMESTAMP)
    ARTICLE_BK: CAST(articleCode as STRING)
hashed_columns:
    ARTICLE_HK:
        - "ARTICLE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "articleCode"
            - "locale"
            - "traceabilityType"
            - "scientificName"
            - "scalesNutrientsDeclaration"
            - "scalesProductName"
            - "scalesGroup"
            - "materialType"
            - "scalesIngredientsDeclaration"
            - "countryOfLastProcessing"
            - "additionallabelinformation"
            - "legalName"
            - "mainStatus"
            - "countryOfOrigin"
            - "articleDescription"
            - "is_created"
            - "is_deleted"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
