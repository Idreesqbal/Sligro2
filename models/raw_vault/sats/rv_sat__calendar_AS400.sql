{%- set yaml_metadata -%}
source_model: "stg_as400__calendpf"
src_pk: "CALENDAR_DATE_NK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "DATEC1"
    - "DGNRC1"
    - "DUM7C1"
    - "EEJJC1"
    - "EJ53C1"
    - "FABEC1"
    - "FABJC1"
    - "FABPC1"
    - "FOEJC1"
    - "FOJJC1"
    - "IFNRC1"
    - "JAARC1"
    - "JR53C1"
    - "KDATC1"
    - "PERIC1"
    - "PR53C1"
    - "SFNRC1"
    - "WEEKC1"
    - "WK53C1"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
src_extra_columns:
    - "END_DATETIME"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
