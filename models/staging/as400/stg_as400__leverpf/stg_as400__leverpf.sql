{%- set yaml_metadata -%}
source_model:
    'base_as400__leverpf'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__leverpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SUPPLIER_BK:
        - "TRIM(CAST(lvnrc1 AS STRING))"
    CARRIER_BK:
        - "TRIM(CAST(lvnrc1 AS STRING))"
    {# Technical changes to source columns #}
    {# Trim string fields #}
    aikdc1:
        - "TRIM(CAST(aikdc1 AS STRING))"
    bkonc1:
        - "TRIM(CAST(bkonc1 AS STRING))"
    btkdc1:
        - "TRIM(CAST(btkdc1 AS STRING))"
    btnrc1:
        - "TRIM(CAST(btnrc1 AS STRING))"
    dbfxc1:
        - "TRIM(CAST(dbfxc1 AS STRING))"
    dbfxc2:
        - "TRIM(CAST(dbfxc2 AS STRING))"
    dbkpc1:
        - "TRIM(CAST(dbkpc1 AS STRING))"
    dbtlc1:
        - "TRIM(CAST(dbtlc1 AS STRING))"
    dbtlc2:
        - "TRIM(CAST(dbtlc2 AS STRING))"
    emalc1:
        - "TRIM(CAST(emalc1 AS STRING))"
    fkkdc1:
        - "TRIM(CAST(fkkdc1 AS STRING))"
    fxnrc1:
        - "TRIM(CAST(fxnrc1 AS STRING))"
    fxnrc2:
        - "TRIM(CAST(fxnrc2 AS STRING))"
    kakdc1:
        - "TRIM(CAST(kakdc1 AS STRING))"
    kwkdc1:
        - "TRIM(CAST(kwkdc1 AS STRING))"
    lakdc1:
        - "TRIM(CAST(lakdc1 AS STRING))"
    ldkdc1:
        - "TRIM(CAST(ldkdc1 AS STRING))"
    lskdc1:
        - "TRIM(CAST(lskdc1 AS STRING))"
    lvadc1:
        - "TRIM(CAST(lvadc1 AS STRING))"
    lvadc2:
        - "TRIM(CAST(lvadc2 AS STRING))"
    lvn7c1:
        - "TRIM(CAST(lvn7c1 AS STRING))"
    lvnmc1:
        - "TRIM(CAST(lvnmc1 AS STRING))"
    lvnmc2:
        - "TRIM(CAST(lvnmc2 AS STRING))"
    lvpkc1:
        - "TRIM(CAST(lvpkc1 AS STRING))"
    lvwpc1:
        - "TRIM(CAST(lvwpc1 AS STRING))"
    lvwpc2:
        - "TRIM(CAST(lvwpc2 AS STRING))"
    mtcac1:
        - "TRIM(CAST(mtcac1 AS STRING))"
    mtcnc1:
        - "TRIM(CAST(mtcnc1 AS STRING))"
    mtcwc1:
        - "TRIM(CAST(mtcwc1 AS STRING))"
    nlkdc1:
        - "TRIM(CAST(nlkdc1 AS STRING))"
    rmkdc1:
        - "TRIM(CAST(rmkdc1 AS STRING))"
    stbdc1:
        - "TRIM(CAST(stbdc1 AS STRING))"
    stikc1:
        - "TRIM(CAST(stikc1 AS STRING))"
    tbkdc1:
        - "TRIM(CAST(tbkdc1 AS STRING))"
    tlnrc1:
        - "TRIM(CAST(tlnrc1 AS STRING))"
    tlnrc2:
        - "TRIM(CAST(tlnrc2 AS STRING))"
    tokdc1:
        - "TRIM(CAST(tokdc1 AS STRING))"
    txabc1:
        - "TRIM(CAST(txabc1 AS STRING))"
    vbkdc1:
        - "TRIM(CAST(vbkdc1 AS STRING))"
    vertc1:
        - "TRIM(CAST(vertc1 AS STRING))"
    vktnc1:
        - "TRIM(CAST(vktnc1 AS STRING))"
    vlkdc1:
        - "TRIM(CAST(vlkdc1 AS STRING))"

hashed_columns:
    SUPPLIER_HK:
        - "SUPPLIER_BK"
    CARRIER_HK:
        - "CARRIER_BK"
    SUPPLIER_CARRIER_HK:
        - "SUPPLIER_BK"
        - "CARRIER_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "lvnrc1"
            - "ADIFC1"
            - "AFPCC1"
            - "AIDEC1"
            - "AIKDC1"
            - "BKNRC1"
            - "BKONC1"
            - "BNFKC1"
            - "BSBYC1"
            - "BTANC1"
            - "BTKDC1"
            - "BTNRC1"
            - "BVFKC1"
            - "CBDGC1"
            - "CBPCC1"
            - "DBFXC1"
            - "DBFXC2"
            - "DBKPC1"
            - "DBTLC1"
            - "DBTLC2"
            - "EMALC1"
            - "FKBAC1"
            - "FKBAC2"
            - "FKKDC1"
            - "FXNRC1"
            - "FXNRC2"
            - "GRNRC1"
            - "IKBAC1"
            - "IKBAC2"
            - "KAKDC1"
            - "KWKDC1"
            - "LAKDC1"
            - "LDKDC1"
            - "LSKDC1"
            - "LVADC1"
            - "LVADC2"
            - "LVIKC1"
            - "LVNMC1"
            - "LVNMC2"
            - "LVN7C1"
            - "LVPKC1"
            - "LVVKC1"
            - "LVWPC1"
            - "LVWPC2"
            - "MTCAC1"
            - "MTCNC1"
            - "MTCWC1"
            - "NLKDC1"
            - "PNFKC1"
            - "PVFKC1"
            - "RMKDC1"
            - "SLDGC1"
            - "SLPCC1"
            - "SRKDC1"
            - "STBDC1"
            - "STIKC1"
            - "S017C1"
            - "TBKDC1"
            - "TLNRC1"
            - "TLNRC2"
            - "TOKDC1"
            - "TXABC1"
            - "TXLDC1"
            - "TXNRC1"
            - "VBKDC1"
            - "VERTC1"
            - "VKTNC1"
            - "VLKDC1"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.stage(**metadata_dict) }}
