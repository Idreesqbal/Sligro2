{%- set yaml_metadata -%}
source_model: "stg_as400__rfkabcpf"
src_pk: "SALES_INVOICE_HK"
src_cdk:
    - "pgmtc1"
src_hashdiff: "HASHDIFF"
src_payload:
    - "abdec1"
    - "abnrc1"
    - "btwac1"
    - "btwbc1"
    - "btwcc1"
    - "btwdc1"
    - "finrc1"
    - "fkbyc1"
    - "fknlc1"
    - "fknoc1"
    - "hkvkc1"
    - "hpayc1"
    - "klfkc1"
    - "klhec1"
    - "klnrc1"
    - "osplc1"
    - "pgmtc1"
    - "pgmxc1"
    - "skvkc1"
    - "program_desc"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ma_sat(**metadata_dict) }}
