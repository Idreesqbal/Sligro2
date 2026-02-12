{%- set yaml_metadata -%}
source_model:
    'base_as400__bsta2cpf'
derived_columns:
    RECORD_SOURCE: "!AS400__bsta2cpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    PURCHASE_ORDER_BK: "TRIM(CAST(ionrc1 AS STRING))"
    GOODS_RECEIVED_BK: "TRIM(CAST(ionrc1 AS STRING))"
    fokdc1: "NULLIF(TRIM(fokdc1), '')"
    ornrc5: "NULLIF(TRIM(ornrc5), '')"
    safrc1: "NULLIF(TRIM(safrc1), '')"
    ionrc1: "UPPER(NULLIF(TRIM(CAST(ionrc1 AS STRING)), ''))"
hashed_columns:
    PURCHASE_ORDER_HK:
        - "PURCHASE_ORDER_BK"
    GOODS_RECEIVED_HK:
        - "GOODS_RECEIVED_BK"

    HASHDIFF:
        is_hashdiff: true
        columns:
            - "finac1"
            - "fokdc1"
            - "ictpc1"
            - "ionrc1"
            - "ornrc4"
            - "ornrc5"
            - "safrc1"
            - "update_ident"
            - "_fivetran_start"
            - "_fivetran_end"

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
