{%- set yaml_metadata -%}
source_model: "stg_as400__dcnawpf"
src_pk: "SITE_HK"
src_hashdiff:
    source_column: "HASHDIFF"
    alias: "HASHDIFF"
src_payload:
    - adrsdn
    - dckddn
    - dcomdn
    - dcpkdn
    - dcstdn
    - dctadn
    - dcwpdn
    - finrdn
    - fxnrdn
    - "END_DATETIME"
    - "START_DATETIME"

src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
