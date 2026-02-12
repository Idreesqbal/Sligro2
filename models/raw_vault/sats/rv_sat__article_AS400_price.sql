{%- set yaml_metadata -%}
source_model: "stg_as400__artikel_prijs"
src_pk: "ARTICLE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "ARTICLE_BK"
    - "artikel_id"
    - "calculatie_prijs_soort_id"
    - "hoofd_kantoor_id"
    - "actueel_bruto_winst"
    - "actueel_bruto_winst_consument_prijs"
    - "actueel_opslag"
    - "actueel_prijs"
    - "actueel_prijs_ingang_datum"
    - "calculatie_batch_nr"
    - "calculatie_prijs_volgorde"
    - "nieuw_bruto_winst"
    - "nieuw_bruto_winst_consument_prijs"
    - "nieuw_consument_prijs"
    - "nieuw_opslag"
    - "nieuw_prijs"
    - "nieuw_prijs_akkoord"
    - "nieuw_prijs_ingang_datum"
    - "prijs_aantal"
    - "prijs_courant_code"
    - "toegevoegd_datum"
    - "toegevoegd_door"
    - "via_gui"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
