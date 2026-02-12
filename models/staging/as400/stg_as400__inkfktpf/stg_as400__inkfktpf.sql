{%- set yaml_metadata -%}
source_model:
    'base_as400__inkfktpf'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__inkfktpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    PURCHASE_INVOICE_BK:
        - "TRIM(CAST(stnrif AS STRING))"
    SUPPLIER_BK:
        - "TRIM(CAST(lvnrif AS STRING))"
    {# Technical changes to source columns #}
    fkdtif: "{{ cast_to_date('fkdtif') }}"
    ovdtif: "{{ cast_to_date('ovdtif') }}"
    vfdtif: "{{ cast_to_date('vfdtif') }}"
    btkdif:
        - "TRIM(CAST(btkdif AS STRING))"
    fkaoif:
        - "TRIM(CAST(fkaoif AS STRING))"
    fkbtif:
        - "TRIM(CAST(fkbtif AS STRING))"
    fknrif:
        - "TRIM(CAST(fknrif AS STRING))"
    ifusif:
        - "TRIM(CAST(ifusif AS STRING))"
    stkdif:
        - "TRIM(CAST(stkdif AS STRING))"
    vakdif:
        - "TRIM(CAST(vakdif AS STRING))"
    vvusif:
        - "TRIM(CAST(vvusif AS STRING))"
hashed_columns:
    PURCHASE_INVOICE_HK:
        - "PURCHASE_INVOICE_BK"
    SUPPLIER_HK:
        - "SUPPLIER_BK"
    PURCHASE_INVOICE_HEADER_TRANSACTIONS_HK:
        - "PURCHASE_INVOICE_BK"
        - "SUPPLIER_BK"

    HASHDIFF:
        is_hashdiff: true
        columns:
            - "aantif"
            - "babdif"
            - "bbbdif"
            - "bdbdif"
            - "bidtif"
            - "btkdif"
            - "bvdtif"
            - "dcaaif"
            - "embdif"
            - "fgdtif"
            - "fkaoif"
            - "fkbdif"
            - "fkbtif"
            - "fkdtif"
            - "fknrif"
            - "grbaif"
            - "grbbif"
            - "hfdkif"
            - "i2dtif"
            - "ifdtif"
            - "ifusif"
            - "inprif"
            - "jfdtif"
            - "jidtif"
            - "jvdtif"
            - "lvnrif"
            - "ogdtif"
            - "ovdtif"
            - "stkdif"
            - "stnrif"
            - "stsuif"
            - "toifif"
            - "toioif"
            - "vabaif"
            - "vabbif"
            - "vabdif"
            - "vafkif"
            - "vakdif"
            - "vfdtif"
            - "vvdtif"
            - "vvusif"
            - "_fivetran_start"
            - "_fivetran_end"

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
