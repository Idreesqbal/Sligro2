{%- set yaml_metadata -%}
source_model:
    'base_as400__thtw5cpf'
derived_columns:
    RECORD_SOURCE: "!AS400__thtw5cpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    STOCK_BK:
        - "TRIM(CAST(PANRC1 AS STRING))"
        - "TRIM(CAST(ARNRC1 AS STRING))"
        - "TRIM(CAST(FINRC1 AS STRING))"
        - "TRIM(CAST(PIBUC1 AS STRING))"
    ARTICLE_BK:
        - "TRIM(CAST(ARNRC1 AS STRING))"
    ARTICLE_GROUP_NK:
        - "TRIM(CAST(ARGRC1 AS STRING))"
    SITE_NK:
        - "TRIM(CAST(FINRC1 AS STRING))"
    COLLECTION_AREA_NK:
        - "TRIM(CAST(FINRC1 AS STRING))"
        - "TRIM(CAST(DCKDC1 AS STRING))"
    STOCK_LOCATION_BK:
        - "TRIM(CAST(FINRC1 AS STRING))"
        - "TRIM(CAST(DCKDC1 AS STRING))"
        - "LPAD(TRIM(CAST(PGNAC1 AS STRING)), 3, '0')"
        - "LPAD(TRIM(CAST(PVNAC1 AS STRING)), 2, '0')"
        - "TRIM(CAST(PLNAC1 AS STRING))"
        - "LPAD(TRIM(CAST(PPNAC1 AS STRING)), 2, '0')"
    SUPPLIER_NK:
        - "TRIM(CAST(LVNRC1 AS STRING))"
    PGNAC1: "LPAD(TRIM(CAST(PGNAC1 AS STRING)), 3, '0')"
    PVNAC1: "LPAD(TRIM(CAST(PVNAC1 AS STRING)), 2, '0')"
    PPNAC1: "LPAD(TRIM(CAST(PPNAC1 AS STRING)), 2, '0')"
hashed_columns:
    STOCK_HK:
        - "STOCK_BK"
    ARTICLE_HK:
        - "ARTICLE_BK"
    ARTICLE_GROUP_HK:
        - "ARTICLE_GROUP_NK"
    SITE_HK:
        - "SITE_NK"
    COLLECTION_AREA_HK:
        - "COLLECTION_AREA_NK"
    STOCK_LOCATION_HK:
        - "STOCK_LOCATION_BK"
    SUPPLIER_HK:
        - "SUPPLIER_NK"
    ARTICLE_LOCATION_SUPPLIER_HK:
        - "ARTICLE_BK"
        - "ARTICLE_GROUP_NK"
        - "SITE_NK"
        - "COLLECTION_AREA_NK"
        - "STOCK_LOCATION_BK"
        - "SUPPLIER_NK"
    ARTICLE_STOCK_HK:
        - ARTICLE_BK
        - STOCK_BK
    SITE_COLLECTION_AREA_STOCK_LOCATION_HK:
        - SITE_NK
        - COLLECTION_AREA_NK
        - STOCK_LOCATION_BK
    STOCK_STOCK_LOCATION_HK:
        - STOCK_BK
        - STOCK_LOCATION_BK
    SUPPLIER_STOCK_HK:
        - SUPPLIER_NK
        - STOCK_BK
    STOCK_HASHDIFF:
        is_hashdiff: true
        columns:
            - "ARTICLE_BK"
            - "PANRC1"
            - "PIBUC1"
            - "THDEC1"
            - "PLAAC3"
            - "REEAC1"
            - "TOTAC1"
            - "TOTDC1"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
