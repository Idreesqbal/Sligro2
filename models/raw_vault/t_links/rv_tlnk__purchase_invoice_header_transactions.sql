{%- set yaml_metadata -%}
source_model: "stg_as400__inkfktpf"
src_pk: "PURCHASE_INVOICE_HEADER_TRANSACTIONS_HK"
src_fk:
    - "PURCHASE_INVOICE_HK"
    - "SUPPLIER_HK"
src_payload:
    - "aantif"
    - "babdif"
    - "bbbdif"
    - "btkdif"
    - "bvdtif"
    - "embdif"
    - "fkbdif"
    - "fgdtif"
    - "ifdtif"
    - "toioif"
    - "vabaif"
    - "vabbif"
    - "vabdif"
    - "vafkif"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.t_link(src_pk=metadata_dict["src_pk"],
                      src_fk=metadata_dict["src_fk"],
                      src_payload=metadata_dict["src_payload"],
                      src_eff=metadata_dict["src_eff"],
                      src_ldts=metadata_dict["src_ldts"],
                      src_source=metadata_dict["src_source"],
                      source_model=metadata_dict["source_model"]) }}
