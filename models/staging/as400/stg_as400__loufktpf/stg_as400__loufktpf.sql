{%- set yaml_metadata -%}
source_model:
    'base_as400__loufktpf'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__loufktpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SALES_INVOICE_BK:
        - "TRIM(CAST(faktnr AS STRING))"
        - "TRIM(CAST(filnr AS STRING))"
        - "TRIM(CAST(factuurdatum AS STRING))"
        - "TRIM(CAST(klanr AS STRING))"
    LEGAL_ENTITY_BK:
        - "TRIM(CAST(fakris AS STRING))"
    SITE_BK:
        - "TRIM(CAST(filnr AS STRING))"
    CUSTOMER_BK:
        - "TRIM(CAST(klanr AS STRING))"
    PAYMENT_BK:
        - "TRIM(CAST(betkod AS STRING))"
    {# Technical changes to source columns #}
    factuurdatum: "{{ cast_to_datetime('factuurdatum',0) }}"
    vrzdat: "{{ cast_to_datetime('vrzdat',0) }}"
    inbkh:
        - "TRIM(CAST(inbkh AS STRING))"
    jnkdlt:
        - "TRIM(CAST(jnkdlt AS STRING))"
    specpr:
        - "TRIM(CAST(specpr AS STRING))"
hashed_columns:
    SALES_INVOICE_HK:
        - "SALES_INVOICE_BK"
    LEGAL_ENTITY_HK:
        - "LEGAL_ENTITY_BK"
    SITE_HK:
        - "SITE_BK"
    CUSTOMER_HK:
        - "CUSTOMER_BK"
    PAYMENT_HK:
        - "PAYMENT_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "faktnr"
            - "filnr"
            - "factuurdatum"
            - "fakris"
            - "klanr"
            - "betkod"
            - "buisnr"
            - "embsal"
            - "inbkh"
            - "jnkdlt"
            - "minuut"
            - "netexc"
            - "netto"
            - "pcnr"
            - "specpr"
            - "srtfak"
            - "uur"
            - "vrzdat"
            - "vrzfak"
            - "_fivetran_start"
            - "_fivetran_end"

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
