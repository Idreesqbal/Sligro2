{%- set yaml_metadata -%}
source_model:
    sligro60_slgmutlib_leverdatum: route_stam_tabel
derived_columns:
    RECORD_SOURCE: "!AS400__route_stam_tabel"
    LOAD_DATETIME: "{{ fivetran_loaddate(["vestiging_id","route"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    ROUTE_NK:
        - "TRIM(CAST(route AS STRING))"
    SITE_NK:
        - "TRIM(CAST(vestiging_id AS STRING))"
hashed_columns:
    ROUTE_HK:
        - "ROUTE_NK"
    SITE_HK:
        - "SITE_NK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "ROUTE_NK"
            - "SITE_NK"
            - "bezorg_dag"
            - "dock_dag"
            - "dock_tijd"
            - "drop_kosten_laad_meters_berekenen"
            - "expeditie_scanning"
            - "export_boord_computer"
            - "export_intertour"
            - "print_wachtende_retour_bonnen"
            - "verplaatsen_naar_route_toegestaan"
            - "verplaatsen_van_route_toegestaan"
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
