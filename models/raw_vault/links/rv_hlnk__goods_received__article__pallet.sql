{%- set yaml_metadata -%}
source_model:
    - "stg_as400__arbvrcpf"
src_pk: "ARTICLE_GOODS_RECEIVED_PALLET_HK"
src_fk:
    - "GOODS_RECEIVED_HK"
    - "ARTICLE_HK"
    - "PALLET_HK"

src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
