{%- set yaml_metadata -%}
source_model: "stg_as400__thtw5cpf"
src_pk: "STOCK_HK"
src_hashdiff:
    source_column: "STOCK_HASHDIFF"
    alias: "HASHDIFF"
src_payload:
    - "PANRC1"
    - "PIBUC1"
    - "THDEC1"
    - "PLAAC3"
    - "REEAC1"
    - "TOTAC1"
    - "TOTDC1"
    - "START_DATETIME"
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
