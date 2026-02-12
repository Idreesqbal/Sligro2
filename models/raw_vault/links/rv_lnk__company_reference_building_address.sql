{%- set yaml_metadata -%}
source_model:
    - "stg_company_info__bag"
src_pk: "COMPANY_BUILDING_ADDRESS_HK"
src_fk:
    - "COMPANY_ADDRESS_HK"
    - "COMPANY_BUILDING_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
