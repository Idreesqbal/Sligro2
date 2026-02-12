{%- set yaml_metadata -%}
source_model: "stg_as400__thtw1cpf"
src_pk: "STOCK_LOCATION_HK"
src_cdk:
    - "VPOMC1"
src_hashdiff:
    source_column: "STOCK_LOCATION_HASHDIFF"
    alias: "HASHDIFF"
src_payload:
    - "STOCK_LOCATION_BK"
    - "FINRC1"
    - "DCKDC1"
    - "PGNRC1"
    - "PVNRC1"
    - "PLNRC2"
    - "PPNRC1"
    - "PIBUC1"
    - "VPOMC1"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ma_sat(**metadata_dict) }}
