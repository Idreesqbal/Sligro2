{%- set yaml_metadata -%}
source_model: "stg_as400__rioifrpf"
src_pk: "RELATION__PURCHASE_INVOICE__PURCHASE_ORDER_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "hfdki1"
    - "START_DATETIME"
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
