{%- set yaml_metadata -%}
source_model: "stg_as400__goedbwpf"
src_pk: "STOCK_TRANSACTION_HK"
src_fk:
    - "ARTICLE_HK"
    - "ARTICLE_GROUP_HK"
    - "SITE_HK"
    - "SUPPLIER_HK"
    - "SALES_ORDER_HK"
src_payload:
    - "ARTICLE_BK"
    - "ARTICLE_GROUP_NK"
    - "SITE_NK"
    - "SUPPLIER_NK"
    - "SALES_ORDER_NK"
    - "AANTGB"
    - "ACKDGB"
    - "ACRFGB"
    - "AFDLGB"
    - "DCAAGB"
    - "KMKDGB"
    - "MTDTGB"
    - "WOKDGB"
    - "START_DATETIME"
    - "END_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.t_link(**metadata_dict) }}
