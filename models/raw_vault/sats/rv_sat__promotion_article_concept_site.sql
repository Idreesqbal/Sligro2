{%- set yaml_metadata -%}
source_model: "stg_as400__fofilcpf"
src_pk: "PROMOTION_ARTICLE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "arnrc1"
    - "askdc1"
    - "finrc1"
    - "foejc1"
    - "fokdc1"
    - "fonrc1"
    - "lvpfc1"
src_extra_columns: "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
