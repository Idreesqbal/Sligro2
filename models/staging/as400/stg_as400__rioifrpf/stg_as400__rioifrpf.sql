{%- set yaml_metadata -%}
source_model:
    'base_as400__rioifrpf'
derived_columns:
    RECORD_SOURCE: "!AS400__rioifrpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    PURCHASE_ORDER_BK:
        - "TRIM(CAST(ionrii AS STRING))"
    PURCHASE_INVOICE_BK:
        - "TRIM(CAST(stnrii AS STRING))"
    RELATION_INVOICE_ORDER_BK:
         - "TRIM(CAST(rinrii AS STRING))"
hashed_columns:
    PURCHASE_ORDER_HK:
        - "PURCHASE_ORDER_BK"
    PURCHASE_INVOICE_HK:
        - "PURCHASE_INVOICE_BK"
    RELATION_INVOICE_ORDER_HK:
        - "RELATION_INVOICE_ORDER_BK"
    RELATION__PURCHASE_INVOICE__PURCHASE_ORDER_HK:
        - "RELATION_INVOICE_ORDER_BK"
        - "PURCHASE_INVOICE_BK"
        - "PURCHASE_ORDER_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "stnrii"
            - "ionrii"
            - "rinrii"
            - "hfdki1"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
