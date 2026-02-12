{%- set yaml_metadata -%}
source_model:
    - "stg_as400__folartpf"
src_pk: "PROMOTION_ARTICLE_HK"
src_fk:
    - "PROMOTION_HK"
    - "ARTICLE_HK"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
