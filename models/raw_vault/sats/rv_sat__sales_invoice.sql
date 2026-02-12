{%- set yaml_metadata -%}
source_model: "stg_as400__dwfktcpf"
src_pk: "SALES_INVOICE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "betmc1"
    - "dvnmc1"
    - "fanrc1"
    - "finrc1"
    - "fkdec1"
    - "fkexc1"
    - "fkrsc1"
    - "fksrc1"
    - "fktdc1"
    - "fktpc1"
    - "jnvfc1"
    - "klnrc9"
    - "okkdc1"
    - "rpscc1"
    - "slorc1"
    - "srorc1"
    - "volgc1"
    - "invoice_type_desc"
    - "currency_code"
    - "START_DATETIME"
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
