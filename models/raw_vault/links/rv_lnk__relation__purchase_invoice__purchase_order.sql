{%- set yaml_metadata -%}
source_model:
    - "stg_as400__rioifrpf"
src_pk: "RELATION__PURCHASE_INVOICE__PURCHASE_ORDER_HK"
src_fk:
    - "PURCHASE_INVOICE_HK"
    - "PURCHASE_ORDER_HK"
    - "RELATION_INVOICE_ORDER_HK"

src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
