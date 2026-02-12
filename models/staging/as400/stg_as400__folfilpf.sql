{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: folfilpf
derived_columns:
    RECORD_SOURCE: "!AS400__folfilpf"
    LOAD_DATETIME: "{{ fivetran_loaddate(["fokdgx","foejgx","fonrgx","arnrgx", "finrgx"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    PROMOTION_ARTICLE_BK:
        - "TRIM(CAST(fokdgx AS STRING))"
        - "TRIM(CAST(foejgx AS STRING))"
        - "TRIM(CAST(fonrgx AS STRING))"
        - "TRIM(CAST(arnrgx AS STRING))"
    PROMOTION_BK:
        - "TRIM(CAST(fokdgx AS STRING))"
        - "TRIM(CAST(foejgx AS STRING))"
        - "TRIM(CAST(fonrgx AS STRING))"
    ARTICLE_BK:
        - "TRIM(CAST(arnrgx AS STRING))"
    SITE_BK:
        - "TRIM(CAST(finrgx AS STRING))"
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
            - "arnrgx"
            - "askdgx"
            - "finrgx"
            - "foejgx"
            - "fokdgx"
            - "fonrgx"
            - "lvpfgx"
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
