{{ config(
    materialized='incremental',
    unique_key= ['COMPANY_REFERENCE_HK', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_company_info__branches"
src_pk: "COMPANY_REFERENCE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "sizokey"
    - "kvk_hoofd_sbi_code"
    - "kvk_hoofd_sbi_code_omschrijving"
    - "kvk_neven_1_sbi_code"
    - "kvk_neven_1_sbi_omschrijving"
    - "kvk_neven_2_sbi_code"
    - "kvk_neven_2_sbi_omschrijving"
    - "kvk_neven_3_sbi_code"
    - "kvk_neven_3_sbi_omschrijving"
    - "company_info1_code"
    - "company_info1_omschrijving"
    - "company_info2_code"
    - "company_info2_omschrijving"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
