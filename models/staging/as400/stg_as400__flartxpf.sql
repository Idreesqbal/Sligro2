{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: flartxpf
derived_columns:
    RECORD_SOURCE: "!AS400__flartxpf"
    LOAD_DATETIME: "{{ fivetran_loaddate(["fokdgt","foejgt","fonrgt","arnrgt"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    PROMOTION_ARTICLE_BK:
        - "TRIM(CAST(fokdgt AS STRING))"
        - "TRIM(CAST(foejgt AS STRING))"
        - "TRIM(CAST(fonrgt AS STRING))"
        - "TRIM(CAST(arnrgt AS STRING))"
    PROMOTION_BK:
        - "TRIM(CAST(fokdgt AS STRING))"
        - "TRIM(CAST(foejgt AS STRING))"
        - "TRIM(CAST(fonrgt AS STRING))"
    ARTICLE_BK:
        - "TRIM(CAST(arnrgt AS STRING))"
hashed_columns:
    PROMOTION_ARTICLE_HK:
        - "PROMOTION_ARTICLE_BK"
    PROMOTION_HK:
        - "PROMOTION_BK"
    ARTICLE_HK:
        - "ARTICLE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "arnrgt"
            - "foejgt"
            - "fokdgt"
            - "fonrgt"
            - "rgnrgt"
            - "tx70gt"
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
