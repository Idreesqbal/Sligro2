{%- set yaml_metadata -%}
source_model:
    - "stg_data_hub_trade_item"
    - "stg_data_hub_trade_item_additional_gtin"
    - "stg_data_hub_trade_item_hierarchy"
src_pk: "TRADE_ITEM_HK"
src_nk: "TRADE_ITEM_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(src_pk=metadata_dict["src_pk"],
                   src_nk=metadata_dict["src_nk"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"]) }}
