{%- set yaml_metadata -%}
source_model:
    'base_data_hub_customer_hierarchy'
hashed_columns:
    CUSTOMER_HK:
        - "CUSTOMER_BK"
    CUSTOMER_PARENT_HK:
        - "parentCustomerId"
    CUSTOMER_HIERARCHY_HK:
        - "hierarchyType"
        - "parentCustomerId"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "customerId"
            - "registrationDate"
            - "hierarchyType"
            - "parentCustomerId"
            - "is_created"
            - "is_deleted"
derived_columns:
    RECORD_SOURCE: "!Data_Hub_Customer"
    LOAD_DATETIME: "event_datetime"
    END_DATETIME: CAST(NULL AS TIMESTAMP)
    CUSTOMER_BK: CAST(customerId AS STRING)
    CUSTOMER_PARENT_BK: "parentCustomerId"
    registrationDate: PARSE_DATE('%Y-%m-%d',registrationDate)
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
