{%- set yaml_metadata -%}
source_model: "stg_as400__inklvcpf"
src_pk: "SUPPLIER_HK"
src_cdk:
    - "EMPLOYEE_BK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "EMPLOYEE_BK"
    - "landcode"
    - "hoofdinkoper"
    - "toegevoegd_door"
    - "toegevoegd_timestamp"
    - "START_DATETIME"
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ma_sat(**metadata_dict) }}
