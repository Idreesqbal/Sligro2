{%- set yaml_metadata -%}
source_model: "stg_as400__folokcpf"
src_pk: "PROMOTION_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "foejc1"
    - "fokdc1"
    - "fonrc1"
    - "onalc1"
    - "onpbc1"
    - "opbdc1"
    - "opbtc1"
    - "opedc1"
    - "opetc1"
src_extra_columns: "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
