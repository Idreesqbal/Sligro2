{%- set yaml_metadata -%}
source_model: stg_company_info__postcode4
src_pk: POSTCODE_HK
src_satellite:
  SATELLITE_POSTCODE:
    sat_name:
      SATELLITE_NAME: SAT_POSTCODE
    hashdiff:
      HASHDIFF: HASHDIFF
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.xts(**metadata_dict) }}
