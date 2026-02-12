{%- set yaml_metadata -%}
source_model:
    'base_as400__inkfrcpf'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__inkfrcpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    PURCHASE_INVOICE_LINE_BK:
        - "TRIM(CAST(stnrc2 AS STRING))"
        - "TRIM(CAST(rgnrc3 AS STRING))"
        - "TRIM(CAST(stkdc1 AS STRING))"
    PURCHASE_INVOICE_BK:
        - "TRIM(CAST(stnrc2 AS STRING))"
    ARTICLE_BK:
        - "TRIM(CAST(arnrc1 AS STRING))"
    {# Technical changes to source columns #}
    stkdc1:
        - "TRIM(CAST(stkdc1 AS STRING))"

hashed_columns:
    PURCHASE_INVOICE_LINE_HK:
        - "PURCHASE_INVOICE_LINE_BK"
    PURCHASE_INVOICE_HK:
        - "PURCHASE_INVOICE_BK"
    ARTICLE_HK:
        - "ARTICLE_BK"
    PURCHASE_INVOICE_LINES_TRANSACTIONS_HK:
        - "PURCHASE_INVOICE_LINE_BK"
        - "ARTICLE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "arnrc1"
            - "bdrgc1"
            - "bdrgc2"
            - "bdrgc3"
            - "bdrgc4"
            - "bdrgc5"
            - "bdrgc6"
            - "bdrgc8"
            - "cydxc1"
            - "ean1c1"
            - "fcaac1"
            - "glaac2"
            - "hfdkc1"
            - "ionrc1"
            - "irnrc1"
            - "pryec1"
            - "rgnrc3"
            - "stkdc1"
            - "stnrc2"
            - "_fivetran_start"
            - "_fivetran_end"

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
