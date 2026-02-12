{%- set yaml_metadata -%}
source_model: "stg_as400__flkencpf"
src_pk: "PROMOTION_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "folder_code"
    - "folder_jaar"
    - "folder_nummer"
    - "kenmerk_code"
    - "kenmerk_waarde"
    - "toegevoegd_datum"
    - "toegevoegd_door"
src_extra_columns: "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
