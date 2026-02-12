{%- set yaml_metadata -%}
source_model: "stg_as400__dwfktcpf"
src_pk: "SALES_INVOICE_HEADER_TRANSACTIONS_HK"
src_fk:
    - "SALES_INVOICE_HK"
    - "SITE_HK"
    - "CUSTOMER_HK"
    - "EMPLOYEE_HK"
    - "PAYMENT_HK"
src_payload:
    - "fktoc1"
    - "fkemc1"
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
