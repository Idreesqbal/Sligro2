{%- set yaml_metadata -%}

source_model:
    'base_as400__arbvrcpf'

derived_columns:
    RECORD_SOURCE: "!AS400__arbvrcpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    PURCHASE_ORDER_BK: "TRIM(CAST(ionrc1 AS STRING))"
    GOODS_RECEIVED_BK: "TRIM(CAST(ionrc1 AS STRING))"
    ARTICLE_BK: "TRIM(CAST(arnrc1 AS STRING))"
    PROMOTION_BK:
        - "TRIM(CAST(if(fokdc1='0',NULL,fokdc1) AS STRING))"
        - "TRIM(CAST(if(foejc1=0,NULL,foejc1) AS STRING))"
        - "TRIM(CAST(if(fonrc1=0,NULL,fonrc1) AS STRING))"
    SITE_BK: "TRIM(CAST(finrc1 AS STRING))"
    PALLET_BK: "TRIM(CAST(PANRC1 AS STRING))"
    dckdc1: "UPPER(NULLIF(TRIM(CAST(dckdc1 AS STRING)), ''))"
    finrc1: "UPPER(NULLIF(TRIM(CAST(finrc1 AS STRING)), ''))"
    ionrc1: "UPPER(NULLIF(TRIM(CAST(ionrc1 AS STRING)), ''))"
    arnrc1: "UPPER(NULLIF(TRIM(CAST(arnrc1 AS STRING)), ''))"
    cydxc1: "{{ sfg_cast_to_date('cydxc1') }}"
    godec1: "{{ sfg_cast_to_date('godec1') }}"
    opdec1: "{{ sfg_cast_to_date('opdec1') }}"
    vedec2: "{{ sfg_cast_to_date('vedec2') }}"
    vetdc2: "{{ sfg_cast_to_date('vetdc2') }}"
    fokdc1: "TRIM(CAST(if(fokdc1='0',NULL,fokdc1) AS STRING))"
    foejc1: "TRIM(CAST(if(foejc1=0,NULL,foejc1) AS STRING))"
    fonrc1: "TRIM(CAST(if(fonrc1=0,NULL,fonrc1) AS STRING))"

hashed_columns:
    PURCHASE_ORDER_HK:
        - "PURCHASE_ORDER_BK"
    GOODS_RECEIVED_HK:
        - "GOODS_RECEIVED_BK"
    GOODS_RECEIVED_ARTICLE_HK:
        - "GOODS_RECEIVED_BK"
        - "ARTICLE_BK"
    ARTICLE_HK:
        - "ARTICLE_BK"
    PROMOTION_HK:
        - "PROMOTION_BK"
    PROMOTION_ARTICLE_GOODS_RECEIVED_PALLET_HK:
        - "PROMOTION_BK"
        - "ARTICLE_BK"
        - "GOODS_RECEIVED_BK"
        - "PALLET_BK"
    SITE_HK:
        - "SITE_BK"
    PALLET_HK:
        - "PALLET_BK"
    ARTICLE_GOODS_RECEIVED_PALLET_HK:
        - "ARTICLE_BK"
        - "GOODS_RECEIVED_BK"
        - "PALLET_BK"

    HASHDIFF:
        is_hashdiff: true
        columns:
            - "arnrc1"
            - "cydxc1"
            - "dckdc1"
            - "eidec1"
            - "fanrc1"
            - "finrc1"
            - "foejc1"
            - "fokdc1"
            - "fonrc1"
            - "godec1"
            - "godfc1"
            - "gotdc1"
            - "gousc1"
            - "ionrc1"
            - "klnrc1"
            - "loknc1"
            - "opdec1"
            - "optdc1"
            - "panrc1"
            - "plaac3"
            - "prtyc1"
            - "rtdec1"
            - "s021c1"
            - "thdec1"
            - "timxc1"
            - "update_ident"
            - "userc1"
            - "usrxc1"
            - "vedec2"
            - "vetdc2"
            - "veusc1"
            - "wnnrc1"
            - "wyejc1"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
