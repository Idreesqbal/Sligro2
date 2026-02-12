{%- set yaml_metadata -%}
source_model:
    sligro60_slgmutlib_centraal: artikel_prijs
derived_columns:
    RECORD_SOURCE: "!AS400__artikel_prijs"
    LOAD_DATETIME: "{{ fivetran_loaddate(["artikel_id"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    ARTICLE_BK:
        - "TRIM(CAST(artikel_id AS STRING))"
hashed_columns:
    ARTICLE_HK:
        - "ARTICLE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
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
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(
      include_source_columns=true,
      source_model=metadata_dict['source_model'],
      derived_columns=metadata_dict['derived_columns'],
      null_columns=none,
      hashed_columns=metadata_dict['hashed_columns'],
      ranked_columns=none
  ) }}
