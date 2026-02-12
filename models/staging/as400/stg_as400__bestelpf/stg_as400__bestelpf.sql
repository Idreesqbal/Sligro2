{%- set yaml_metadata -%}

source_model:
    'base_as400__bestelpf'

derived_columns:
    RECORD_SOURCE: "!AS400__bestelpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    PURCHASE_ORDER_LINE_BK: "TRIM(CAST(irnrbe AS STRING))"
    GOODS_RECEIVED_LINE_BK: "TRIM(CAST(irnrbe AS STRING))"
    PURCHASE_ORDER_BK: "TRIM(CAST(ionrbe AS STRING))"
    GOODS_RECEIVED_BK: "TRIM(CAST(ionrbe AS STRING))"
    ARTICLE_BK: "TRIM(CAST(arnrbe AS STRING))"
    SUPPLIER_BK: "TRIM(CAST(lvnrbe AS STRING))"
    godtbe: "{{ cast_to_date('godtbe') }}"
    bkdtbe: "{{ cast_to_date('bkdtbe') }}"
    akkdbe: "NULLIF(TRIM(akkdbe), '')"
    arombe: "NULLIF(TRIM(arombe), '')"
    askdbe: "NULLIF(TRIM(askdbe), '')"
    bdbtbe: "NULLIF(TRIM(bdbtbe), '')"
    bkusbe: "NULLIF(TRIM(bkusbe), '')"
    btkdbe: "NULLIF(TRIM(btkdbe), '')"
    etikbe: "NULLIF(TRIM(etikbe), '')"
    fekdbe: "NULLIF(TRIM(fekdbe), '')"
    feusbe: "NULLIF(TRIM(feusbe), '')"
    gousbe: "NULLIF(TRIM(gousbe), '')"
    oskdbe: "NULLIF(TRIM(oskdbe), '')"
    stikbe: "NULLIF(TRIM(stikbe), '')"
    vpombe: "NULLIF(TRIM(vpombe), '')"
    arnrbe: "UPPER(NULLIF(TRIM(CAST(arnrbe AS STRING)), ''))"
    lvnrbe: "UPPER(NULLIF(TRIM(CAST(lvnrbe AS STRING)), ''))"
    ionrbe: "UPPER(NULLIF(TRIM(CAST(ionrbe AS STRING)), ''))"
    irnrbe: "UPPER(NULLIF(TRIM(CAST(irnrbe AS STRING)), ''))"

hashed_columns:
    PURCHASE_ORDER_LINE_HK:
        - "PURCHASE_ORDER_LINE_BK"
    GOODS_RECEIVED_LINE_HK:
        - "GOODS_RECEIVED_LINE_BK"
    PURCHASE_ORDER_HK:
        - "PURCHASE_ORDER_BK"
    GOODS_RECEIVED_HK:
        - "PURCHASE_ORDER_BK"
    ARTICLE_HK:
        - "ARTICLE_BK"
    SUPPLIER_HK:
        - "SUPPLIER_BK"
    GOODS_RECEIVED_LINE_ARTICLE_HK:
        - "GOODS_RECEIVED_BK"
        - "GOODS_RECEIVED_LINE_BK"
        - "ARTICLE_BK"
    GOODS_RECEIVED_ARTICLE_HK:
        - "GOODS_RECEIVED_BK"
        - "ARTICLE_BK"
    PURCHASE_ORDER_LINE_ARTICLE_HK:
        - "PURCHASE_ORDER_BK"
        - "PURCHASE_ORDER_LINE_BK"
        - "ARTICLE_BK"

    HASHDIFF:
        is_hashdiff: true
        columns:
            - "acbdbe"
            - "aiprbe"
            - "akkdbe"
            - "argrbe"
            - "arnrbe"
            - "arombe"
            - "askdbe"
            - "bdbtbe"
            - "bkdtbe"
            - "bkusbe"
            - "bsaabe"
            - "btkdbe"
            - "cnprbe"
            - "dcaabe"
            - "emprbe"
            - "etaabe"
            - "etikbe"
            - "fedtbe"
            - "fekdbe"
            - "feusbe"
            - "gewibe"
            - "godtbe"
            - "gousbe"
            - "inprbe"
            - "ionrbe"
            - "irnrbe"
            - "ivaabe"
            - "jgdtbe"
            - "kobdbe"
            - "kopcbe"
            - "lvnrbe"
            - "odaabe"
            - "oskdbe"
            - "ovaabe"
            - "ovkgbe"
            - "rdkdbe"
            - "stikbe"
            - "tbaabe"
            - "tibdbe"
            - "toaabe"
            - "vpombe"
            - "vrklbe"
            - "zbprbe"
            - "_fivetran_start"
            - "_fivetran_end"
            - "btkdbe_description"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
