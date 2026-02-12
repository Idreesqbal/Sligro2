{%- set yaml_metadata -%}
source_model: "stg_bom__klant_zb_67_relatie"
src_pk: "CUSTOMER_BK"
src_extra_columns:
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
    - "START_DATETIME"
    - "END_DATETIME"
    - "HASHDIFF"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ref_table(**metadata_dict) }}
