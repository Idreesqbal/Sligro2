{{
  config(
    materialized = 'incremental',
    unique_key = ['CUSTOMER_HK', 'CONTACT_PERSON_HK','LOAD_DATETIME'],
    apply_source_filter = true
    )
}}

{%- set yaml_metadata -%}
source_model: "stg_data_hub_customer_contact_person"
src_pk:
  - "CONTACT_PERSON_HK"
  - "CUSTOMER_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "customerId"
    - "registrationDate"
    - "contactPersonId"
    - "countryLanguageCode"
    - "state"
    - "contactpersonType"
    - "initials"
    - "firstName"
    - "preposition"
    - "lastName"
    - "salutation"
    - "sex"
    - "dateOfBirth"
    - "emailAddress"
    - "phoneNumberWork"
    - "phoneNumberHome"
    - "phoneNumberMobile"
    - "street"
    - "houseNumber"
    - "houseNumberAddition"
    - "postalCode"
    - "city"
    - "countryCode"
    - "jobPosition"
    - "is_deleted"
    - "is_created"
src_ldts: "LOAD_DATETIME"
src_extra_columns:
    - "END_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
