{%- set yaml_metadata -%}
source_model: "stg_as400__leverancier_intercompany"
src_pk:
    - "SUPPLIER_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "fis_crediteur_nr"
    - "filiaal_nummer_hoofdkantoor"
    - "toegevoegd_datum"
    - "toegevoegd_door"
    - "verstuur_email"
    - "eliminatie_boeking_aanmaken"
    - "leverancier_status"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.sat(**metadata_dict) }}
