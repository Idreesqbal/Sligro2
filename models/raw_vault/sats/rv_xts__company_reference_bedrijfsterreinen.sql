{%- set yaml_metadata -%}
source_model: stg_company_info__bedrijfsterreinen
src_pk: POSTCODE_HK
src_satellite:
  SATELLITE_COMMERCIAL_AREA:
    sat_name:
      SATELLITE_NAME: MASAT_COMMERCIAL_AREA
    hashdiff:
      HASHDIFF: HASHDIFF
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
src_extra_columns:
    - "sizopcd"
    - "sizobtrvnr"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.xts(**metadata_dict) }}
