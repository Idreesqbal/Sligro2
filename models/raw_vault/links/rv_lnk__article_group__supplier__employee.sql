{%- set yaml_metadata -%}
source_model:
    - "stg_as400__rel_voorraad_beheerder_artikel_groep"
src_pk: "ARTICLE_GROUP_SUPPLIER_EMPLOYEE_HK"
src_fk:
    - "ARTICLE_GROUP_HK"
    - "SUPPLIER_HK"
    - "EMPLOYEE_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(src_pk=metadata_dict["src_pk"],
                    src_fk=metadata_dict["src_fk"],
                    src_ldts=metadata_dict["src_ldts"],
                    src_source=metadata_dict["src_source"],
                    source_model=metadata_dict["source_model"]) }}
