{%- set yaml_metadata -%}
source_model:
    'base_data_hub_customer_contact_person'
hashed_columns:
    CUSTOMER_HK:
        - "CUSTOMER_BK"
    CONTACT_PERSON_HK:
        - "CONTACT_PERSON_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
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
            - "is_created"
            - "is_deleted"
derived_columns:
    RECORD_SOURCE: "!Data_Hub_Customer"
    LOAD_DATETIME: "event_datetime"
    END_DATETIME: CAST(NULL AS TIMESTAMP)
    CUSTOMER_BK: CAST(customerId AS STRING)
    CONTACT_PERSON_BK: CAST(contactPersonId as STRING)
    registrationDate: PARSE_DATE('%Y-%m-%d',registrationDate)
    dateOfBirth: PARSE_DATE('%Y-%m-%d',dateOfBirth)
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
