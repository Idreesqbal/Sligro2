{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: flvzflpf
derived_columns:
    RECORD_SOURCE: "!AS400__flvzflpf"
    LOAD_DATETIME: "{{ fivetran_loaddate(["arn1ij","foejij","fokdij","fonrij"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    PROMOTION_BK:
        - "TRIM(CAST(fokdij AS STRING))"
        - "TRIM(CAST(foejij AS STRING))"
        - "TRIM(CAST(fonrij AS STRING))"
    PROMOTION_ARTICLE_BK:
        - "TRIM(CAST(foejij AS STRING))"
        - "TRIM(CAST(fokdij AS STRING))"
        - "TRIM(CAST(fonrij AS STRING))"
        - "TRIM(CAST(arn1ij AS STRING))"
hashed_columns:
    PROMOTION_HK:
        - "PROMOTION_BK"
    PROMOTION_ARTICLE_HK:
        - "PROMOTION_ARTICLE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "arn1ij"
            - "foejij"
            - "fokdij"
            - "fonrij"
            - "arnrij"
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
