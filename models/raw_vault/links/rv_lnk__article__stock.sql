{%- set yaml_metadata -%}
source_model:
    - "stg_as400__thtw1cpf"
    - "stg_as400__thtw5cpf"
src_pk: "ARTICLE_STOCK_HK"
src_fk:
    - "STOCK_HK"
    - "ARTICLE_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
