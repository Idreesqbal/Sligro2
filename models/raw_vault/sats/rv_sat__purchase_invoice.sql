

{%- set yaml_metadata -%}
source_model: "stg_as400__inkfktpf"
src_pk: "PURCHASE_INVOICE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "BIDTIF"
    - "BTKDIF"
    - "BVDTIF"
    - "FGDTIF"
    - "FKAOIF"
    - "FKBTIF"
    - "FKDTIF"
    - "FKNRIF"
    - "GRBAIF"
    - "GRBBIF"
    - "HFDKIF"
    - "IFDTIF"
    - "IFUSIF"
    - "I2DTIF"
    - "JFDTIF"
    - "JIDTIF"
    - "JVDTIF"
    - "OGDTIF"
    - "OVDTIF"
    - "STKDIF"
    - "STSUIF"
    - "TOIFIF"
    - "VFDTIF"
    - "VVDTIF"
    - "VVUSIF"
    - "VAKDIF"
    - "START_DATETIME"
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
