{%- set yaml_metadata -%}
source_model:
    'base_data_hub_customer_address'
hashed_columns:
    CUSTOMER_HK:
        - "CUSTOMER_BK"
    CUSTOMER_ADDRESS_HK:
        - "CUSTOMER_ADDRESS_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "customerId"
            - "registrationDate"
            - "attentionOf"
            - "addressId"
            - "addressType"
            - "addressStatus"
            - "addressName"
            - "street"
            - "houseNumber"
            - "houseNumberAddition"
            - "postalCode"
            - "city"
            - "countryCode"
            - "emailAddress"
            - "phoneNumber"
            - "phoneNumberMobile"
            - "is_created"
            - "is_deleted"
derived_columns:
    RECORD_SOURCE: "!Data_Hub_Customer"
    LOAD_DATETIME: "event_datetime"
    END_DATETIME: CAST(NULL AS TIMESTAMP)
    CUSTOMER_BK: CAST(customerId AS STRING)
    CUSTOMER_ADDRESS_BK: CAST(addressId as STRING)
    registrationDate: PARSE_DATE('%Y-%m-%d',registrationDate)
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
