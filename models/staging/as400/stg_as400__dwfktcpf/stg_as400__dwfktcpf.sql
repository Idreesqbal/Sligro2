{%- set yaml_metadata -%}
source_model:
    'base_as400__dwfktcpf'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__dwfktcpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SALES_INVOICE_BK:
        - "TRIM(CAST(fanrc1 AS STRING))"
        - "TRIM(CAST(finrc1 AS STRING))"
        - "TRIM(CAST(fkdec1 AS STRING))"
        - "TRIM(CAST(klnrc9 AS STRING))"
    SITE_BK:
        - "TRIM(CAST(finrc1 AS STRING))"
    CUSTOMER_BK:
        - "TRIM(CAST(klnrc9 AS STRING))"
    EMPLOYEE_BK:
        - "TRIM(CAST(fkrsc1 AS STRING))"
    PAYMENT_BK:
        - "TRIM(CAST(betmc1 AS STRING))"
    {# Technical changes to source columns #}
    FKDEC1: "{{ cast_to_datetime('FKDEC1','FKTDC1') }}"
    dvnmc1:
        - "TRIM(CAST(dvnmc1 AS STRING))"
    fkexc1:
        - "TRIM(CAST(fkexc1 AS STRING))"
    jnvfc1:
        - "TRIM(CAST(jnvfc1 AS STRING))"
    okkdc1:
        - "TRIM(CAST(okkdc1 AS STRING))"
    slorc1:
        - "TRIM(CAST(slorc1 AS STRING))"
hashed_columns:
    SALES_INVOICE_HK:
        - "SALES_INVOICE_BK"
    SITE_HK:
        - "SITE_BK"
    CUSTOMER_HK:
        - "CUSTOMER_BK"
    EMPLOYEE_HK:
        - "EMPLOYEE_BK"
    PAYMENT_HK:
        - "PAYMENT_BK"
    SALES_INVOICE_HEADER_TRANSACTIONS_HK:
        - "SALES_INVOICE_BK"
        - "CUSTOMER_BK"
        - "SITE_BK"
        - "EMPLOYEE_BK"
        - "PAYMENT_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "betmc1"
            - "dvnmc1"
            - "fanrc1"
            - "finrc1"
            - "fkdec1"
            - "fkemc1"
            - "fkexc1"
            - "fkrsc1"
            - "fksrc1"
            - "fktdc1"
            - "fktoc1"
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
            - "_fivetran_start"
            - "_fivetran_end"

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
