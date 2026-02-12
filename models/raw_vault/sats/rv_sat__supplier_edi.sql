{%- set yaml_metadata -%}
source_model: "stg_as400__lvedicpf"
src_pk: "SUPPLIER_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "bstmc1"
    - "vrobc1"
    - "START_DATETIME"
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
