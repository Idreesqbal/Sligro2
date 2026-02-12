{%- set yaml_metadata -%}
source_model:
    - "stg_company_info__instellingen"
src_pk: "COMPANY_REFERENCE_ADDRESS_POSTCODE_HK"
src_fk:
    - "COMPANY_REFERENCE_HK"
    - "COMPANY_ADDRESS_HK"
    - "POSTCODE_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
