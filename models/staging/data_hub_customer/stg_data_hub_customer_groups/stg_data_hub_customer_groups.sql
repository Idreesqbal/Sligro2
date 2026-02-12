{%- set yaml_metadata -%}
source_model:
    'base_data_hub_customer_groups'
hashed_columns:
    CUSTOMER_HK:
        - "CUSTOMER_BK"
    CUSTOMER_GROUP_HK:
        - "CUSTOMER_GROUP_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "customerId"
            - "registrationDate"
            - "description"
            - "groupId"
            - "typeCode"
            - "typeDescription"
            - "is_created"
            - "is_deleted"
derived_columns:
    RECORD_SOURCE: "!Data_Hub_Customer"
    LOAD_DATETIME: "event_datetime"
    END_DATETIME: CAST(NULL AS TIMESTAMP)
    CUSTOMER_BK: CAST(customerId AS STRING)
    CUSTOMER_GROUP_BK: CAST(groupId as STRING)
    registrationDate: PARSE_DATE('%Y-%m-%d',registrationDate)
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
