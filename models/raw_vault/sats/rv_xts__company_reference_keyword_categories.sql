{%- set yaml_metadata -%}
source_model: stg_company_info__keyword_categories
src_pk: COMPANY_REFERENCE_HK
src_satellite:
  SATELLITE_KEYWORD_CATEGORIES:
    sat_name:
      SATELLITE_NAME: MASAT_KEYWORD_CATEGORIES
    hashdiff:
      HASHDIFF: HASHDIFF
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
src_extra_columns:
    - "sizolockey"
    - "klantkey"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.xts(**metadata_dict) }}
