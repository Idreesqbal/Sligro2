{%- set yaml_metadata -%}
source_model: stg_company_info__bag
src_pk: COMPANY_ADDRESS_HK
src_satellite:
  SATELLITE_ADDRESS:
    sat_name:
      SATELLITE_NAME: SAT_ADDRESS
    hashdiff:
      HASHDIFF: HASHDIFF
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
src_extra_columns:
    - "sizolocidm"
    - "bagopndid"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.xts(**metadata_dict) }}
