{%- set yaml_metadata -%}
source_model:
    'base_as400__lvinkcpf'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__lvinkcpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SUPPLIER_BK:
        - "TRIM(CAST(lvnrc1 AS STRING))"
    {# Technical changes to source columns #}
    accmc1:
        - "TRIM(CAST(accmc1 AS STRING))"
    accmc2:
        - "TRIM(CAST(accmc2 AS STRING))"
    accmc3:
        - "TRIM(CAST(accmc3 AS STRING))"
    accmc4:
        - "TRIM(CAST(accmc4 AS STRING))"
    accmc5:
        - "TRIM(CAST(accmc5 AS STRING))"
    cntpc1:
        - "TRIM(CAST(cntpc1 AS STRING))"
    cntpc2:
        - "TRIM(CAST(cntpc2 AS STRING))"
    cntpc3:
        - "TRIM(CAST(cntpc3 AS STRING))"
    emalc2:
        - "TRIM(CAST(emalc2 AS STRING))"
    emalc3:
        - "TRIM(CAST(emalc3 AS STRING))"
    emalc4:
        - "TRIM(CAST(emalc4 AS STRING))"
    emalc5:
        - "TRIM(CAST(emalc5 AS STRING))"
    fomsc1:
        - "TRIM(CAST(fomsc1 AS STRING))"
    fomsc2:
        - "TRIM(CAST(fomsc2 AS STRING))"
    fomsc3:
        - "TRIM(CAST(fomsc3 AS STRING))"
    lvadc2:
        - "TRIM(CAST(lvadc2 AS STRING))"
    lvadc3:
        - "TRIM(CAST(lvadc3 AS STRING))"
    lvwpc2:
        - "TRIM(CAST(lvwpc2 AS STRING))"
    WEBSC2:
        - "TRIM(CAST(WEBSC2 AS STRING))"
hashed_columns:
    SUPPLIER_HK:
        - "SUPPLIER_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "lvnrc1"
            - "ACCMC1"
            - "ACCMC2"
            - "ACCMC3"
            - "ACCMC4"
            - "ACCMC5"
            - "CNTPC1"
            - "CNTPC2"
            - "CNTPC3"
            - "EMALC2"
            - "EMALC3"
            - "EMALC4"
            - "EMALC5"
            - "FOMSC1"
            - "FOMSC2"
            - "FOMSC3"
            - "FXNRC2"
            - "FXNRC3"
            - "FXNRC4"
            - "LVADC2"
            - "LVADC3"
            - "LVPKC2"
            - "LVWPC2"
            - "MBNRC2"
            - "TLNRC2"
            - "TLNRC3"
            - "TLNRC4"
            - "TLNRC5"
            - "WEBSC2"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
