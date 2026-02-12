{%- set yaml_metadata -%}
source_model: stg_company_info__bag_pand
src_pk: COMPANY_BUILDING_HK
src_satellite:
  SATELLITE_BUILDING:
    sat_name:
      SATELLITE_NAME: SAT_BUILDING
    hashdiff:
      HASHDIFF: HASHDIFF
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.xts(**metadata_dict) }}
