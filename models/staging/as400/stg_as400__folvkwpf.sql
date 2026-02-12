{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: folvkwpf
derived_columns:
    RECORD_SOURCE: "!AS400__folvkwpf"
    LOAD_DATETIME: "{{ fivetran_loaddate(["fokdhy","foejhy","fonrhy","arnrhy"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    PROMOTION_ARTICLE_BK:
        - "TRIM(CAST(fokdhy AS STRING))"
        - "TRIM(CAST(foejhy AS STRING))"
        - "TRIM(CAST(fonrhy AS STRING))"
        - "TRIM(CAST(arnrhy AS STRING))"
    ARTICLE_BK:
        - "TRIM(CAST(arnrhy AS STRING))"
    PROMOTION_BK:
        - "TRIM(CAST(fokdhy AS STRING))"
        - "TRIM(CAST(foejhy AS STRING))"
        - "TRIM(CAST(fonrhy AS STRING))"
hashed_columns:
    PROMOTION_ARTICLE_HK:
        - "PROMOTION_ARTICLE_BK"
    ARTICLE_HK:
        - "ARTICLE_BK"
    PROMOTION_HK:
        - "PROMOTION_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "aanthy"
            - "arnrhy"
            - "arn1hy"
            - "foejhy"
            - "fokdhy"
            - "fonrhy"
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
