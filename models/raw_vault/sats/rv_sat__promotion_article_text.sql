{%- set yaml_metadata -%}
source_model: "stg_as400__flaancpf"
src_pk: "PROMOTION_ARTICLE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "arnrc1"
    - "cydxc1"
    - "foejc1"
    - "fokdc1"
    - "fonrc1"
    - "kodec1"
    - "mcprc1"
    - "mctkc1"
    - "mcvac1"
src_extra_columns: "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
