{%- set yaml_metadata -%}
source_model:
    - "stg_as400__catlvcpf"
    - "stg_as400__inklvcpf"
    - "stg_as400__leverpf"
    - "stg_as400__lvedicpf"
    - "stg_as400__lvfiscpf"
    - "stg_as400__lvinkcpf"

src_pk: "SUPPLIER_HK"
src_nk: "SUPPLIER_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(src_pk=metadata_dict["src_pk"],
                   src_nk=metadata_dict["src_nk"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"]) }}
