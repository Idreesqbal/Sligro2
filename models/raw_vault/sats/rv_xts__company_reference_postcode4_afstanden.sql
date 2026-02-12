{%- set yaml_metadata -%}
source_model: stg_company_info__postcode4_afstanden
src_pk: POSTCODE_FROM_TO_HK
src_satellite:
  SATELLITE_DISTANCE:
    sat_name:
      SATELLITE_NAME: SAT_DISTANCE
    hashdiff:
      HASHDIFF: HASHDIFF
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.xts(**metadata_dict) }}
