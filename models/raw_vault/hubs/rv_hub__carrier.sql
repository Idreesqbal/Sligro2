{%- set yaml_metadata -%}
source_model:
    - "stg_as400__transporteur"
src_pk: "CARRIER_HK"
src_nk: "CARRIER_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(**metadata_dict) }}
