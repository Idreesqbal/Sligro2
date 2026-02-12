{%- set yaml_metadata -%}
source_model:
    - "stg_as400__btw_codering"
src_pk: "BTW_BK"
src_extra_columns:
    - "btw_id"
    - "land_id"
    - "gewijzigd_datum"
    - "btw_oms"
    - "mutatie_teller"
    - "toegevoegd_datum"
    - "toegevoegd_door"
    - "orchestra_btw_code"
    - "gewijzigd_door"
    - "START_DATETIME"
    - "END_DATETIME"
    - "HASHDIFF"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ref_table(**metadata_dict) }}
