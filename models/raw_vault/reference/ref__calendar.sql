{%- set yaml_metadata -%}
source_model:
    - "stg_calendar__date"
src_pk: "CALENDAR_DATE_NK"
src_extra_columns:
    - "prior_date_day"
    - "next_date_day"
    - "prior_year_date_day"
    - "prior_year_over_year_date_day"
    - "day_of_week"
    - "day_of_week_iso"
    - "day_of_week_name"
    - "day_of_week_name_short"
    - "day_of_month"
    - "day_of_year"
    - "week_start_date"
    - "first_day_of_week_name"
    - "week_end_date"
    - "prior_year_week_start_date"
    - "prior_year_week_end_date"
    - "week_of_year"
    - "iso_week_start_date"
    - "iso_first_day_of_week_name"
    - "iso_week_end_date"
    - "prior_year_iso_week_start_date"
    - "prior_year_iso_week_end_date"
    - "iso_week_of_year"
    - "prior_year_week_of_year"
    - "prior_year_iso_week_of_year"
    - "month_of_year"
    - "month_name"
    - "month_name_short"
    - "month_start_date"
    - "month_end_date"
    - "prior_year_month_start_date"
    - "prior_year_month_end_date"
    - "quarter_of_year"
    - "quarter_start_date"
    - "quarter_end_date"
    - "year_number"
    - "year_start_date"
    - "year_end_date"
    - "iso_four_week_period"
    - "year_of_week"
    - "iso_year_of_week"
    - "last_day_of_month_indicator"
    - "iso_weekday_weekend_indicator"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ref_table(src_pk=metadata_dict["src_pk"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_extra_columns=metadata_dict["src_extra_columns"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"]) }}
