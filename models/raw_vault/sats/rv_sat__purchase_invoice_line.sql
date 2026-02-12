{%- set yaml_metadata -%}
source_model: "stg_as400__inkfrcpf"
src_pk: "PURCHASE_INVOICE_LINES_TRANSACTIONS_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "arnrc1"
    - "ean1c1"
    - "hfdkc1"
    - "ionrc1"
    - "irnrc1"
    - "rgnrc3"
    - "stkdc1"
    - "START_DATETIME"
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
