{{ config(
    materialized='incremental',
    unique_key= ['CALENDAR_DATE_NK', 'holiday_name', 'holiday_type', 'country_region', 'country', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_gcs_manual_imports__calendar_national_holiday"
src_pk:
    - "CALENDAR_DATE_NK"
    - "holiday_name"
    - "holiday_type"
    - "country_region"
    - "country"
src_hashdiff: "HASHDIFF"
src_payload:
    - "holiday_name"
    - "holiday_type"
    - "holiday_start_date"
    - "holiday_end_date"
    - "country_region"
    - "country"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
src_extra_columns:
    - "END_DATETIME"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
