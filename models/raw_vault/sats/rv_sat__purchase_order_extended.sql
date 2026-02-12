{%- set yaml_metadata -%}
source_model: "stg_as400__bsta2cpf"
src_pk: "PURCHASE_ORDER_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "finac1"
    - "fokdc1"
    - "ictpc1"
    - "ionrc1"
    - "ornrc4"
    - "ornrc5"
    - "safrc1"
    - "update_ident"
    - "START_DATETIME"
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
