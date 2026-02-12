{%- set yaml_metadata -%}
source_model:
    - "stg_as400__thtw1cpf"
    - "stg_as400__thtw5cpf"
src_pk: "STOCK_STOCK_LOCATION_HK"
src_dfk: "STOCK_HK"
src_sfk: "STOCK_LOCATION_HK"
src_start_date: "START_DATETIME"
src_end_date: "END_DATETIME"
src_eff: "EFFECTIVE_FROM"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.eff_sat(**metadata_dict) }}
