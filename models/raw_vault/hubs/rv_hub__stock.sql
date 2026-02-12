{%- set yaml_metadata -%}
source_model:
    - "stg_as400__thtw1cpf"
    - "stg_as400__thtw5cpf"
src_pk: "STOCK_HK"
src_nk: "STOCK_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(**metadata_dict) }}
