{%- set yaml_metadata -%}
source_model:
    - "stg_data_hub_customer_address"
src_pk: "CUSTOMER_ADDRESS_HK"
src_nk: "CUSTOMER_ADDRESS_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(**metadata_dict) }}
