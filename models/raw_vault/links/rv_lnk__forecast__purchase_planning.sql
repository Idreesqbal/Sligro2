{%- set yaml_metadata -%}
source_model:
    - "stg_slim4__bs_forecast"
    - "stg_slim4__cdc_forecast"
src_pk: "FORECAST_HK"
src_fk:
    - "ARTICLE_HK"
    - "SITE_HK"
    - "SUPPLIER_HK"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(**metadata_dict) }}
