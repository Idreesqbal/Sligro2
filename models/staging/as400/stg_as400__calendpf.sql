{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: calendpf
derived_columns:
    RECORD_SOURCE: "!AS400__calendpf"
    LOAD_DATETIME: "{{ fivetran_loaddate(["KDATC1"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    CALENDAR_DATE_NK: "PARSE_DATE('%Y%m%d', TRIM(CAST(KDATC1 AS STRING)))"
hashed_columns:
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "DATEC1"
            - "DGNRC1"
            - "DUM7C1"
            - "EEJJC1"
            - "EJ53C1"
            - "FABEC1"
            - "FABJC1"
            - "FABPC1"
            - "FOEJC1"
            - "FOJJC1"
            - "IFNRC1"
            - "JAARC1"
            - "JR53C1"
            - "KDATC1"
            - "PERIC1"
            - "PR53C1"
            - "SFNRC1"
            - "WEEKC1"
            - "WK53C1"
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
