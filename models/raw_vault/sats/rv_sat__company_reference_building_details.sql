{{ config(
    materialized='incremental',
    unique_key= ['COMPANY_BUILDING_HK', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_company_info__bag_pand"
src_pk: "COMPANY_BUILDING_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "bagopndid"
    - "sizcbedver"
    - "pecwozb"
    - "pecwpndih"
    - "peckpndih"
    - "pecwtuiop"
    - "pecwdakop"
    - "peckdakop"
    - "pecwpndho"
    - "peckpndho"
    - "peccdak"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
