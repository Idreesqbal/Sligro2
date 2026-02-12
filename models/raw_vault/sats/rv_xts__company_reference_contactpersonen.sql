{%- set yaml_metadata -%}
source_model: stg_company_info__contactpersonen
src_pk: COMPANY_REFERENCE_HK
src_satellite:
  SATELLITE_CONTACT_PERSON:
    sat_name:
      SATELLITE_NAME: MASAT_CONTACT_PERSON
    hashdiff:
      HASHDIFF: HASHDIFF
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
src_extra_columns:
    - "sizocpkey"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.xts(**metadata_dict) }}
