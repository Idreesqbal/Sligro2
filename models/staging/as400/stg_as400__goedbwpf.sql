{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: goedbwpf
derived_columns:
    RECORD_SOURCE: "!AS400__goedbwpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    ARTICLE_BK:
        - "TRIM(CAST(ARNRGB AS STRING))"
    ARTICLE_GROUP_NK:
        - "TRIM(CAST(ARGRGB AS STRING))"
    SITE_NK:
        - "!{{ var("cdc_sitecode") }}" # HR.001
    SUPPLIER_NK:
        - "TRIM(CAST(LVNRGB AS STRING))"
    SALES_ORDER_NK:
        - "TRIM(CAST(ORNRGB AS STRING))"
hashed_columns:
    ARTICLE_HK:
        - "ARTICLE_BK"
    ARTICLE_GROUP_HK:
        - "ARTICLE_GROUP_NK"
    SITE_HK:
        - "SITE_NK"
    SUPPLIER_HK:
        - "SUPPLIER_NK"
    SALES_ORDER_HK:
        - "SALES_ORDER_NK"
    STOCK_TRANSACTION_HK:
        - "ARTICLE_BK"
        - "ARTICLE_GROUP_NK"
        - "SITE_NK"
        - "SUPPLIER_NK"
        - "SALES_ORDER_NK"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
