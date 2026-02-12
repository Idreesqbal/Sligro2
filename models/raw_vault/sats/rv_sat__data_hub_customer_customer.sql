{{
  config(
    materialized = 'incremental',
    unique_key = ['CUSTOMER_HK','LOAD_DATETIME']
    )
}}

{%- set yaml_metadata -%}
source_model: "stg_data_hub_customer"
src_pk: "CUSTOMER_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "customerId"
    - "tradeName"
    - "subsegmentDescription"
    - "subsegmentCode"
    - "statusReasonDescription"
    - "segmentDescription"
    - "segmentCode"
    - "sbiCode"
    - "sbiDescription"
    - "SBI2CODE_STRING"
    - "SBI2DESCRIPTION_STRING"
    - "formulaType"
    - "customerOf"
    - "registrationDate"
    - "headOfficeIndicator"
    - "origin"
    - "statusCode"
    - "nationalAccountLevel"
    - "nationalAccountSegment"
    - "nationalAccountDescription"
    - "statusDescription"
    - "owner"
    - "statutoryName"
    - "legalForm"
    - "chamberOfCommerceNumber"
    - "chamberOfCommerceEstablishmentNumber"
    - "countryCode"
    - "category"
    - "accountManagerEmployeeId"
    - "is_deleted"
    - "is_created"
src_ldts: "LOAD_DATETIME"
src_extra_columns:
    - "END_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
