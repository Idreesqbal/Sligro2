{{ config(
    materialized='incremental',
    unique_key= ['TRADE_ITEM_HK', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_data_hub_trade_item"
src_pk: "TRADE_ITEM_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "unitOfMeasure"
    - "packagingwidthuom"
    - "packagingHeight"
    - "netweightuom"
    - "netcontentsuom"
    - "netWeight"
    - "netContents"
    - "packagingDepth"
    - "numberOfBaseUnits"
    - "drainedweightuom"
    - "legacyArticleNumberSligroM"
    - "legacyArticleNumberSligro"
    - "grossWeight"
    - "packagingheightuom"
    - "legacyArticleNumberAanTafel"
    - "packagingWidth"
    - "grossweightuom"
    - "packagingdepthuom"
    - "drainedWeight"
    - "legacyArticleNumberSligroISPC"
    - "is_deleted"
    - "is_created"
src_ldts: "LOAD_DATETIME"
src_extra_columns:
    - "END_DATETIME"
    - "is_created"
    - "is_deleted"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
