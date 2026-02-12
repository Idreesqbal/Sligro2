{%- set yaml_metadata -%}
source_model: "stg_as400__inkfrcpf"
src_pk: "PURCHASE_INVOICE_LINES_TRANSACTIONS_HK"
src_fk:
    - "PURCHASE_INVOICE_LINE_HK"
    - "PURCHASE_INVOICE_HK"
    - "ARTICLE_HK"
src_payload:
    - "bdrgc1"
    - "bdrgc2"
    - "bdrgc3"
    - "bdrgc4"
    - "bdrgc5"
    - "bdrgc6"
    - "bdrgc8"
    - "fcaac1"
    - "glaac2"
    - "pryec1"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.t_link(**metadata_dict) }}
