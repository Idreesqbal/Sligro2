{%- set yaml_metadata -%}
source_model:
    - "stg_data_hub_article"
src_pk: "ARTICLE_HK"
src_nk: "ARTICLE_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(**metadata_dict) }}
