{%- set yaml_metadata -%}
source_model:
    - "stg_as400__transporteur"
src_pk: "CARRIER_SITE_HK"
src_fk:
    - "CARRIER_HK"
    - "SITE_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
