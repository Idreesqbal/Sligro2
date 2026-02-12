{%- set yaml_metadata -%}
source_model: "stg_as400__filia2pf"
src_pk: "SITE_HK"
src_hashdiff:
    source_column: "HASHDIFF"
    alias: "HASHDIFF"
src_payload:
    - dtdac1
    - dtdbc1
    - dtsac1
    - dtsbc1
    - finrc1
    - frmlc1
    - losoc1
    - s006c1
    - synmc1
    - todrc1
    - todtc1
    - vsdac1
    - vsdbc1
    - vssac1
    - vssbc1
    - wzdrc1
    - wzdtc1

src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
