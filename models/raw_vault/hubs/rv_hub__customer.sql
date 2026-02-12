{%- set yaml_metadata -%}
source_model:
    - "stg_company_info__matching_table"
    - "stg_data_hub_customer"
src_pk: "CUSTOMER_HK"
src_nk: "CUSTOMER_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(**metadata_dict) }}
