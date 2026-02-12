{%- set yaml_metadata -%}
source_model:
    'base_gcs_manual_imports__calendar_national_holiday'
derived_columns:
    RECORD_SOURCE: "!GCS_MANUAL_IMPORTS__calendar_national_holiday"
    LOAD_DATETIME: "CURRENT_TIMESTAMP()"
    CALENDAR_DATE_NK: "calendar_date_nk"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
hashed_columns:
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "holiday_name"
            - "holiday_type"
            - "holiday_start_date"
            - "holiday_end_date"
            - "country_region"
            - "country"
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
