{%- set yaml_metadata -%}
source_model:
    - "stg_as400__folartpf"
    - "stg_as400__folkoppf"
    - "stg_as400__flaancpf"
    - "stg_as400__flartxpf"
    - "stg_as400__flkencpf"
    - "stg_as400__flvzflpf"
    - "stg_as400__fofilcpf"
    - "stg_as400__folfilpf"
    - "stg_as400__folokcpf"
    - "stg_as400__folprspf"
    - "stg_as400__folvkwpf"
src_pk: "PROMOTION_HK"
src_nk: "PROMOTION_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(src_pk=metadata_dict["src_pk"],
                   src_nk=metadata_dict["src_nk"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"]) }}
