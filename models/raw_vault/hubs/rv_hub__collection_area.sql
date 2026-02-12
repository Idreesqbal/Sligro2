{%- set yaml_metadata -%}
source_model:
    - "stg_as400__artikcpf"
    - "stg_as400__dcarecpf"
    - "stg_as400__thtw1cpf"
    - "stg_as400__thtw5cpf"
    - "stg_as400__verzamel_gebied_vestiging"
src_pk: "COLLECTION_AREA_HK"
src_nk: "COLLECTION_AREA_NK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(**metadata_dict) }}
