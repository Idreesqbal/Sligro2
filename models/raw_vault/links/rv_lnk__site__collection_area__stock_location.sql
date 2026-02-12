{%- set yaml_metadata -%}
source_model:
    - "stg_as400__thtw1cpf"
    - "stg_as400__thtw5cpf"
    # TODO: stg_as400__artikcpf, stg_as400__dcarecpf, stg_as400__verzamel_gebied_vestiging
src_pk: "SITE_COLLECTION_AREA_STOCK_LOCATION_HK"
src_fk:
    - "SITE_HK"
    - "COLLECTION_AREA_HK"
    - "STOCK_LOCATION_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
