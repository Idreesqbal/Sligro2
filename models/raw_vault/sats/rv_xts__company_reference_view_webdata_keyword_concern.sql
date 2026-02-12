{%- set yaml_metadata -%}
source_model: stg_company_info__view_webdata_keywords_concern
src_pk: COMPANY_REFERENCE_HK
src_satellite:
  SATELLITE_KEYWORD:
    sat_name:
      SATELLITE_NAME: SAT_KEYWORD
    hashdiff:
      HASHDIFF: HASHDIFF
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.xts(**metadata_dict) }}
