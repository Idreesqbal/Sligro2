{%- set yaml_metadata -%}
source_model:
    - "stg_as400__filialpf"
src_pk: "SITE_HEADOFFICE_HK"
src_fk:
    - "SITE_HK"
    - "HEADOFFICE_HK"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
