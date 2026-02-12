{%- set yaml_metadata -%}
source_model:
    - stg_data_hub_customer_hierarchy
src_pk: CUSTOMER_HK
src_fk: CUSTOMER_PARENT_HK
src_extra_columns:
    - hierarchyType
    - is_deleted
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
