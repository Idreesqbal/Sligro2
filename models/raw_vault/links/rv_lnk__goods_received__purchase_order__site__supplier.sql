{%- set yaml_metadata -%}
source_model:
    - "stg_as400__bstalgpf"
src_pk: "GOODS_RECEIVED_PURCHASE_ORDER_SITE_SUPPLIER_HK"
src_fk:
    - "GOODS_RECEIVED_HK"
    - "PURCHASE_ORDER_HK"
    - "SITE_HK"
    - "SUPPLIER_HK"

src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
