{%- set yaml_metadata -%}
source_model:
    'base_as400__dwfkrcpf'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__dwfkrcpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SALES_INVOICE_LINE_BK:
        - "TRIM(CAST(fanrc1 AS STRING))"
        - "TRIM(CAST(finrc1 AS STRING))"
        - "TRIM(CAST(fkdec1 AS STRING))"
        - "TRIM(CAST(klnrc9 AS STRING))"
        - "TRIM(CAST(volgc1 AS STRING))"
        - "TRIM(CAST(arnrc1 AS STRING))"
        - "TRIM(CAST(psnrc1 AS STRING))"
    SALES_INVOICE_BK:
        - "TRIM(CAST(fanrc1 AS STRING))"
        - "TRIM(CAST(finrc1 AS STRING))"
        - "TRIM(CAST(fkdec1 AS STRING))"
        - "TRIM(CAST(klnrc9 AS STRING))"
    PROMOTION_BK:
        - "TRIM(CAST(fokdc1 AS STRING))"
        - "TRIM(CAST(foejc1 AS STRING))"
        - "TRIM(CAST(fonrc1 AS STRING))"
    ARTICLE_BK:
        - "TRIM(CAST(arnrc1 AS STRING))"
    PRICE_BK:
        - "TRIM(CAST(prnrc1 AS STRING))"
    SITE_BK:
        - "TRIM(CAST(finrc1 AS STRING))"
    {# Technical changes to source columns #}
    fkdec1: "{{ cast_to_datetime('fkdec1',0) }}"
    lvdec1: "{{ cast_to_datetime('lvdec1',0) }}"
    rtdec1: "{{ cast_to_datetime('rtdec1',0) }}"
    ackdc1:
        - "TRIM(CAST(ackdc1 AS STRING))"
    acrfc1:
        - "TRIM(CAST(acrfc1 AS STRING))"
    btkdc1:
        - "TRIM(CAST(btkdc1 AS STRING))"
    cdalc1:
        - "TRIM(CAST(cdalc1 AS STRING))"
    cretc1:
        - "TRIM(CAST(cretc1 AS STRING))"
    dckdc1:
        - "TRIM(CAST(dckdc1 AS STRING))"
    dvnmc1:
        - "TRIM(CAST(dvnmc1 AS STRING))"
    dwjnc1:
        - "TRIM(CAST(dwjnc1 AS STRING))"
    fokdc1:
        - "TRIM(CAST(fokdc1 AS STRING))"
    initc1:
        - "TRIM(CAST(initc1 AS STRING))"
    msorc1:
        - "TRIM(CAST(msorc1 AS STRING))"
    okkdc1:
        - "TRIM(CAST(okkdc1 AS STRING))"
    osbrc1:
        - "TRIM(CAST(osbrc1 AS STRING))"
    osjnc1:
        - "TRIM(CAST(osjnc1 AS STRING))"
    pokdc1:
        - "TRIM(CAST(pokdc1 AS STRING))"

hashed_columns:
    SALES_INVOICE_LINE_HK:
        - "SALES_INVOICE_LINE_BK"
    SALES_INVOICE_HK:
        - "SALES_INVOICE_BK"
    PROMOTION_HK:
        - "PROMOTION_BK"
    ARTICLE_HK:
        - "ARTICLE_BK"
    PRICE_HK:
        - "PRICE_BK"
    SITE_HK:
        - "SITE_BK"
    SALES_INVOICE_LINES_TRANSACTIONS_HK:
        - "SALES_INVOICE_LINE_BK"
        - "SALES_INVOICE_BK"
        - "PROMOTION_BK"
        - "ARTICLE_BK"
        - "PRICE_BK"
        - "SITE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "acaac1"
            - "ackbc1"
            - "ackdc1"
            - "acrfc1"
            - "alcpc1"
            - "alnrc1"
            - "aonrc1"
            - "arnrc1"
            - "brpxc1"
            - "bsaac1"
            - "btkdc1"
            - "cdalc1"
            - "cdnvc1"
            - "cnprc1"
            - "cokdc1"
            - "cretc1"
            - "dcaac1"
            - "dckdc1"
            - "dfpxc1"
            - "dvnmc1"
            - "dwjnc1"
            - "emprc1"
            - "fanrc1"
            - "finrc1"
            - "fkaac1"
            - "fkdec1"
            - "fksrc1"
            - "foejc1"
            - "fokdc1"
            - "fonrc1"
            - "glaac1"
            - "ilitc1"
            - "initc1"
            - "inprc1"
            - "kgaac1"
            - "kgbsc1"
            - "klnrc9"
            - "klnuc1"
            - "kopcc1"
            - "koprc1"
            - "lvdec1"
            - "msorc1"
            - "okkdc1"
            - "osbdc1"
            - "osbrc1"
            - "osjnc1"
            - "pokdc1"
            - "portc1"
            - "prnrc1"
            - "psnrc1"
            - "rdcdc1"
            - "rdkbc1"
            - "rtdec1"
            - "rtnrc1"
            - "spnrc1"
            - "volgc1"
            - "vzvoc1"
            - "zbbxc1"
            - "zbprc1"
            - "_fivetran_start"
            - "_fivetran_end"

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
