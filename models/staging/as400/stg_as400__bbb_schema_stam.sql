{%- set yaml_metadata -%}
source_model:
    sligro60_slgmutlib_leverdatum: bbb_schema_stam
derived_columns:
    RECORD_SOURCE: "!AS400__bbb_schema_stam"
    LOAD_DATETIME: "CURRENT_TIMESTAMP()"
    EFFECTIVE_FROM: "{{ cast_to_datetime('gewijzigd_datum',0) }}"
    END_DATE: "{{ fivetran_enddate() }}"
    EFFECTIVE_FROM: "gewijzigd_datum"
    LOAD_DATETIME: "{{ fivetran_loaddate(["bbb_schema_id", "route", "klant_id", "vestiging_id"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    BBB_SCHEME_NK:
        - "TRIM(CAST(bbb_schema_id AS STRING))"
    ROUTE_NK:
        - "TRIM(CAST(route AS STRING))"
    CUSTOMER_NK:
        - "TRIM(CAST(klant_id AS STRING))"
    SITE_NK:
        - "TRIM(CAST(vestiging_id AS STRING))"
hashed_columns:
    BBB_SCHEME_HK:
        - "BBB_SCHEME_NK"
    ROUTE_HK:
        - "ROUTE_NK"
    CUSTOMER_HK:
        - "CUSTOMER_NK"
    SITE_HK:
        - "SITE_NK"
    BBB_SCHEME_ROUTE_HK:
        - "BBB_SCHEME_NK"
        - "ROUTE_NK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "BBB_SCHEME_NK"
            - "ROUTE_NK"
            - "CUSTOMER_NK"
            - "SITE_NK"
            - "route_soort"
            - "bestel_dag"
            - "bestel_tijd"
            - "bezorg_dag"
            - "bezorg_tijd"
            - "datum_geldig_tot"
            - "datum_geldig_van"
            - "bellen_dag"
            - "bellen_tijd"
            - "aantal_dagen_benaderen_bellen"
            - "aantal_dagen_bestellen_voor"
            - "email_dag"
            - "email_tijd"
            - "aantal_dagen_benaderen_email"
            - "slimis_dag"
            - "slimis_tijd"
            - "aantal_dagen_benaderen_slimis"
            - "sms_dag"
            - "sms_tijd"
            - "aantal_dagen_benaderen_sms"
            - "folder"
            - "folder_voor_tijdens"
            - "max_wagen_type"
            - "min_wagen_type"
            - "soort_schema"
            - "vaste_stop_tijd"
            - "venster_dag_1_tot"
            - "venster_dag_1_van"
            - "venster_dag_2_tot"
            - "Venster_dag_2_van"
            - "venster_tijd_1_tot"
            - "venster_tijd_1_van"
            - "venster_tijd_2_tot"
            - "venster_tijd_2_van"
            - "percentage"
            - "actief"
            - "status_code"
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
