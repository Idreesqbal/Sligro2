{%- set yaml_metadata -%}
source_model:
    - "stg_as400__btw_codering_tarief"
src_pk: "BTW_BK"
src_extra_columns:
    - "geldig_vanaf"
    - "geldig_tm"
    - "btw_percentage"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ref_table(**metadata_dict) }}
