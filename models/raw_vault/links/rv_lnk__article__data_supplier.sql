{%- set yaml_metadata -%}
source_model:
    - "stg_article_data_supplier"
src_pk: "RPE_SUPPLIER_HK"
src_fk:
    - "ARTICLE_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
