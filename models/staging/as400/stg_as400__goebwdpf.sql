{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: goebwdpf
derived_columns:
    RECORD_SOURCE: "!AS400__goebwdpf"
    LOAD_DATETIME: "_fivetran_synced"
    END_DATETIME: "!NULL"
    ARTICLE_BK:
        - "ARNRGB"
    STOCK_TRANSACTION_CODE:
        - "MTDEGB"
        - "ARNRGB"
        - "MUKDGB"
hashed_columns:
    ARTICLE_HK:
        - "ARTICLE_BK"
    STOCK_TRANSACTION_HK:
        - "STOCK_TRANSACTION_CODE"
    HASHDIFF:
        is_hashdiff: true
        exclude_columns: true
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(
      include_source_columns=true,
      source_model=metadata_dict['source_model'],
      derived_columns=metadata_dict['derived_columns'],
      null_columns=none,
      hashed_columns=metadata_dict['hashed_columns'],
      ranked_columns=none
  ) }}
