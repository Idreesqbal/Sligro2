{%- set yaml_metadata -%}
source_model: stg_company_info__matching_table
src_pk: COMPANY_REFERENCE_CUSTOMER_HK
src_satellite:
  SATELLITE_CUSTOMER_MATCH:
    sat_name:
      SATELLITE_NAME: SAT_CUSTOMER_MATCH
    hashdiff:
      HASHDIFF: HASHDIFF
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
src_extra_columns:
    - "sizokey"
    - "klantkey"
    - "startdatum_klant"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.xts(**metadata_dict) }}
