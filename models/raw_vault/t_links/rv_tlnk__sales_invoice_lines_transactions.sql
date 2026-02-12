{%- set yaml_metadata -%}
source_model: "stg_as400__dwfkrcpf"
src_pk: "SALES_INVOICE_LINES_TRANSACTIONS_HK"
src_fk:
    - "SALES_INVOICE_LINE_HK"
    - "SALES_INVOICE_HK"
    - "PROMOTION_HK"
    - "ARTICLE_HK"
    - "PRICE_HK"
    - "SITE_HK"
src_payload:
    - "koprc1"
    - "inprc1"
    - "zbbxc1"
    - "zbprc1"
    - "cnprc1"
    - "emprc1"
    - "kopcc1"
    - "brpxc1"
    - "glaac1"
    - "fkaac1"
    - "kgaac1"
    - "kgbsc1"
    - "ilitc1"
    - "portc1"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.t_link(**metadata_dict) }}
