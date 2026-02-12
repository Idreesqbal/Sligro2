{%- set yaml_metadata -%}
source_model:
    sligro60_db260_centraal: artikel_magazijn_locatie
derived_columns:
    RECORD_SOURCE: "!AS400__artikel_magazijn_locatie"
    LOAD_DATETIME: "{{ fivetran_loaddate(["DC_ID", "DC_AREA_ID", "DC_GANG_NR", "DC_VAK_NR", "DC_LAAG_NR", "DC_PLAATS_NR"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    STOCK_LOCATION_BK:
        - "!{{ var("cdc_sitecode") }}" # HR.001
        - "DC_ID"
        - "DC_AREA_ID"
        - "DC_GANG_NR"
        - "DC_VAK_NR"
        - "DC_LAAG_NR"
        - "DC_PLAATS_NR"
    ARTICLE_BK:
        - "TRIM(CAST(ARTIKEL_ID AS STRING))"
hashed_columns:
    STOCK_LOCATION_HK:
        - "STOCK_LOCATION_BK"
    AREA_HK:
        - "DC_ID"
        - "DC_AREA_ID"
    SITE_HK:
        - "!5006" # HR.001
    ARTICLE_HK:
        - "ARTICLE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "artikel_id"
            - "locatie_soort"
            - "dc_area_id"
            - "dc_gang_nr"
            - "dc_id"
            - "dc_laag_nr"
            - "dc_plaats_nr"
            - "dc_vak_nr"
            - "max_locatie_aantal"
            - "min_locatie_aantal"
            - "opslag_methode_id"
            - "ptl_verhuizing"
            - "toegevoegd_datum"
            - "toegevoegd_door"
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
