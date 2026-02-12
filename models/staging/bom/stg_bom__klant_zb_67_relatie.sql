{%- set yaml_metadata -%}
source_model:
    sligro60_slgmutlib_bom: klant_zb_67_relatie
derived_columns:
    RECORD_SOURCE: "!BOM__klant_zb_67_relatie"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    CUSTOMER_BK:
        - "TRIM(CAST(naar_7_klant_id AS STRING))"
    orig_klant_id: "TRIM(CAST(orig_klant_id AS STRING))"

hashed_columns:
    CUSTOMER_HK:
        - "CUSTOMER_BK"

    HASHDIFF:
            is_hashdiff: true
            columns:
                - "orig_klant_id"
                - "orig_creatie_datum"
                - "naar_7_klant_id"
                - "gewijzigd_datum"
                - "orig_verval_datum"
                - "naar_7_verval_datum"
                - "klant_verzamel_nr"
                - "naar_6_verval_datum"
                - "toegevoegd_datum"
                - "toegevoegd_door"
                - "naar_6_opschoon_datum"
                - "orig_creatie_datum"
                - "gewijzigd_door"
                - "mutatie_teller"
                - "naar_7_creatie_datum"
                - "klanten_kaart_print_datum_gepland"
                - "naar_6_creatie_datum"
                - "orig_opschoon_datum"
                - "naar_6_klant_id"
                - "orig_klant_id"
                - "_fivetran_start"
                - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
