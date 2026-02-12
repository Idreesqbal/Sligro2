{%- set yaml_metadata -%}
source_model:
    'base_as400__rfkabcpf'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__rfkabcpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SALES_INVOICE_BK:
        - "TRIM(CAST(abnrc1 AS STRING))"
        - "TRIM(CAST(finrc1 AS STRING))"
        - "TRIM(CAST(abdec1 AS STRING))"
        - "TRIM(CAST(klnrc1 AS STRING))"
    CUSTOMER_BK:
        - "TRIM(CAST(klnrc1 AS STRING))"
    SITE_BK:
        - "TRIM(CAST(finrc1 AS STRING))"
    {# Technical changes to source columns #}
    abdec1: "{{ cast_to_datetime('abdec1',0) }}"
    dattc1: "{{ cast_to_datetime('dattc1',0) }}"
    fkbyc1:
        - "TRIM(CAST(fkbyc1 AS STRING))"
    osplc1:
        - "TRIM(CAST(osplc1 AS STRING))"
    pgmtc1:
        - "TRIM(CAST(pgmtc1 AS STRING))"
    pgmxc1:
        - "TRIM(CAST(pgmxc1 AS STRING))"
    usrtc1:
        - "TRIM(CAST(usrtc1 AS STRING))"
hashed_columns:
    SALES_INVOICE_HK:
        - "SALES_INVOICE_BK"
    CUSTOMER_HK:
        - "CUSTOMER_BK"
    SITE_HK:
        - "SITE_BK"

    HASHDIFF:
        is_hashdiff: true
        columns:
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
            - "_fivetran_start"
            - "_fivetran_end"

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
