{%- set yaml_metadata -%}
source_model: "stg_as400__artikel_btw"
src_pk: "ARTICLE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "ARTICLE_BK"
    - "land_id"
    - "btw_id"
    - "toegevoegd_datum"
    - "toegevoegd_door"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
