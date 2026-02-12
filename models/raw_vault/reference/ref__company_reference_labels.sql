{%- set yaml_metadata -%}
source_model:
    - "stg_company_info__labels"
src_pk: "COMPANY_REFERENCE_LABELS_BK"
src_extra_columns:
    - "identifier"
    - "veldnaam"
    - "sizclabgr"
    - "volgorde"
    - "sizovolg"
    - "sizwlab"
    - "siztlab"
    - "siztlabeng"
    - "crud"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ref_table(src_pk=metadata_dict["src_pk"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_extra_columns=metadata_dict["src_extra_columns"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"]) }}
