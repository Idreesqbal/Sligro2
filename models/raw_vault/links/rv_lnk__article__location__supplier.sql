{%- set yaml_metadata -%}
source_model:
    - "stg_as400__artikcpf"
    - "stg_as400__thtw1cpf"
    - "stg_as400__thtw5cpf"
src_pk: "ARTICLE_LOCATION_SUPPLIER_HK"
src_fk:
    - "ARTICLE_HK"
    - "ARTICLE_GROUP_HK"
    - "SITE_HK"
    - "COLLECTION_AREA_HK"
    - "STOCK_LOCATION_HK"
    - "SUPPLIER_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(src_pk=metadata_dict["src_pk"],
                    src_fk=metadata_dict["src_fk"],
                    src_ldts=metadata_dict["src_ldts"],
                    src_source=metadata_dict["src_source"],
                    source_model=metadata_dict["source_model"]) }}
