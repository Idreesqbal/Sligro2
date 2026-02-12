{%- set yaml_metadata -%}

source_model:
    - "stg_as400__bstalgpf"

src_pk: "GOODS_RECEIVED_HK"
src_nk: "GOODS_RECEIVED_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(**metadata_dict) }}
