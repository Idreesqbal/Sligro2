{%- set yaml_metadata -%}
source_model:
    sligro60_db260_financien: leverancier_intercompany
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__leverancier_intercompany"
    LOAD_DATETIME: "{{ fivetran_loaddate(["leverancier_id"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    SUPPLIER_BK:
        - "TRIM(CAST(leverancier_id AS STRING))"
    {# Technical changes to source columns #}
    eliminatie_boeking_aanmaken:
        - "TRIM(CAST(eliminatie_boeking_aanmaken AS STRING))"
    toegevoegd_door:
        - "TRIM(CAST(toegevoegd_door AS STRING))"
hashed_columns:
    SUPPLIER_HK:
        - "SUPPLIER_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "leverancier_id"
            - "fis_crediteur_nr"
            - "filiaal_nummer_hoofdkantoor"
            - "toegevoegd_datum"
            - "toegevoegd_door"
            - "verstuur_email"
            - "eliminatie_boeking_aanmaken"
            - "leverancier_status"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
