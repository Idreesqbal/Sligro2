{%- set yaml_metadata -%}
source_model:
    - "stg_as400__dwfkrcpf"
    - "stg_as400__dwfktcpf"
    - "stg_as400__rfkabcpf"
    - "stg_as400__loufktpf"
src_pk: "SALES_INVOICE_HK"
src_nk: "SALES_INVOICE_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(src_pk=metadata_dict["src_pk"],
                   src_nk=metadata_dict["src_nk"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"]) }}
