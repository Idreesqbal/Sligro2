{%- set yaml_metadata -%}
source_model:
    - "stg_company_info__instellingen"
    - "stg_company_info__concerns"
    - "stg_company_info__branches"
    - "stg_company_info__contactpersonen"
    - "stg_company_info__sizokeys_opgeheven"
    - "stg_company_info__view_webdata_keywords_concern"
    - "stg_company_info__keyword_categories"
    - "stg_company_info__matching_table"
src_pk: "COMPANY_REFERENCE_HK"
src_nk: "COMPANY_REFERENCE_BK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(src_pk=metadata_dict["src_pk"],
                   src_nk=metadata_dict["src_nk"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"]) }}
