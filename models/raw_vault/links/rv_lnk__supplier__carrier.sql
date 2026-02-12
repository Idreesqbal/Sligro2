{%- set yaml_metadata -%}
source_model:
    - "stg_as400__transporteur"
src_pk: "SUPPLIER_CARRIER_HK"
src_fk:
    - "SUPPLIER_HK"
    - "CARRIER_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
