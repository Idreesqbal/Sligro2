{%- set yaml_metadata -%}
source_model:
    'base_data_hub_customer'
hashed_columns:
    CUSTOMER_HK:
        - "CUSTOMER_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "customerId"
            - "registrationDate"
            - "tradeName"
            - "subsegmentDescription"
            - "subsegmentCode"
            - "statusReasonDescription"
            - "segmentDescription"
            - "segmentCode"
            - "sbiCode"
            - "sbiDescription"
            - "formulaType"
            - "customerOf"
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
            - "is_created"
            - "is_deleted"
derived_columns:
    RECORD_SOURCE: "!Data_Hub_Customer"
    LOAD_DATETIME: "event_datetime"
    END_DATETIME: CAST(NULL AS TIMESTAMP)
    CUSTOMER_BK: CAST(customerId AS STRING)
    SBI2CODE_STRING: (SELECT STRING_AGG(CAST(el AS STRING), ',') FROM UNNEST(sbi2Code) el)
    SBI2DESCRIPTION_STRING: ARRAY_TO_STRING(sbi2Description, ',')
    registrationDate: PARSE_DATE('%Y-%m-%d',registrationDate)
    tradeName: UPPER(tradeName)
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
