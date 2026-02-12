{%- set yaml_metadata -%}
source_model:
    - "stg_data_hub_customer_contact_person"
src_pk: "CONTACT_PERSON_HK"
src_nk: "CONTACT_PERSON_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(**metadata_dict) }}
