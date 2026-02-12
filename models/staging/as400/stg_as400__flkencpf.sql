{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: flkencpf
derived_columns:
    RECORD_SOURCE: "!AS400__flkencpf"
    LOAD_DATETIME: "{{ fivetran_loaddate(["folder_code","folder_jaar","folder_nummer"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    PROMOTION_BK:
        - "TRIM(CAST(folder_code AS STRING))"
        - "TRIM(CAST(folder_jaar AS STRING))"
        - "TRIM(CAST(folder_nummer AS STRING))"
hashed_columns:
    PROMOTION_HK:
        - "PROMOTION_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "folder_code"
            - "folder_jaar"
            - "folder_nummer"
            - "kenmerk_code"
            - "kenmerk_waarde"
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
