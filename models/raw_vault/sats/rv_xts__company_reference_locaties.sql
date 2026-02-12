{%- set yaml_metadata -%}
source_model: stg_company_info__locaties
src_pk: COMPANY_REFERENCE_LOCATION_HK
src_satellite:
  SATELLITE_LOCATION:
    sat_name:
      SATELLITE_NAME: MASAT_LOCATION
    hashdiff:
      HASHDIFF: HASHDIFF
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
src_extra_columns:
    - "sizolockey"
    - "POSTCODE6"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.xts(**metadata_dict) }}
