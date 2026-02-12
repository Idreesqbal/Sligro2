{%- set yaml_metadata -%}
source_model:
    'base_article_data_supplier'
hashed_columns:
    ARTICLE_HK:
        - "ARTICLE_BK"
    RPE_SUPPLIER_HK:
        - "RPE_SUPPLIER_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "article_id"
            - "article_ownership_nl"
            - "article_ownership_be"
            - "unit"
            - "supplier_id"
            - "supplier_name"
            - "supplier_gln"
derived_columns:
    RECORD_SOURCE: "!STEP"
    LOAD_DATETIME: "publish_time"
    END_DATETIME: CAST(NULL AS TIMESTAMP)
    ARTICLE_BK: CAST(article_id AS STRING)
    RPE_SUPPLIER_BK: coalesce(supplier_id,'Empty')
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
