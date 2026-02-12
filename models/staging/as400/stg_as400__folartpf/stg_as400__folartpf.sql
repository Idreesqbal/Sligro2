{%- set yaml_metadata -%}
source_model:
    'base_as400__folartpf_explode_verzamelartikel'
derived_columns:
    RECORD_SOURCE: "!AS400__folartpf"
    LOAD_DATETIME: "{{ fivetran_loaddate(["fokdgu", "foejgu", "fonrgu", "article_code"]) }}"
    EFFECTIVE_FROM: "vndegu"
    END_DATETIME: "{{ fivetran_enddate() }}"
    PROMOTION_ARTICLE_BK:
        - "TRIM(CAST(fokdgu AS STRING))"
        - "TRIM(CAST(foejgu AS STRING))"
        - "TRIM(CAST(fonrgu AS STRING))"
        - "TRIM(CAST(article_code AS STRING))"
    PROMOTION_BK:
        - "TRIM(CAST(fokdgu AS STRING))"
        - "TRIM(CAST(foejgu AS STRING))"
        - "TRIM(CAST(fonrgu AS STRING))"
    ARTICLE_BK:
        - "TRIM(CAST(article_code AS STRING))"
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
            - "acopgu"
            - "apntgu"
            - "arnrgu"
            - "argrgu"
            - "article_code"
            - "babdgu"
            - "blkdgu"
            - "bvbdgu"
            - "claagu"
            - "dtdogu"
            - "fokdgu"
            - "foejgu"
            - "fonrgu"
            - "intkgu"
            - "jnkdgu"
            - "kakdgu"
            - "koomgu"
            - "lcnrgu"
            - "lvnrgu"
            - "lykdgu"
            - "mtdegu"
            - "mttdgu"
            - "mtusgu"
            - "mtwsgu"
            - "oms3gu"
            - "oms4gu"
            - "orbvgu"
            - "pfnrgu"
            - "prblgu"
            - "promgu"
            - "tmdegu"
            - "vndegu"
            - "zgaagu"
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
