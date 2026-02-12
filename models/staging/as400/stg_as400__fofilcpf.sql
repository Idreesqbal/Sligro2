{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: fofilcpf
derived_columns:
    RECORD_SOURCE: "!AS400__fofilcpf"
    LOAD_DATETIME: "{{ fivetran_loaddate(('fokdc1','foejc1','fonrc1','arnrc1')) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    PROMOTION_ARTICLE_BK:
        - "TRIM(CAST(fokdc1 AS STRING))"
        - "TRIM(CAST(foejc1 AS STRING))"
        - "TRIM(CAST(fonrc1 AS STRING))"
        - "TRIM(CAST(arnrc1 AS STRING))"
    PROMOTION_BK:
        - "TRIM(CAST(fokdc1 AS STRING))"
        - "TRIM(CAST(foejc1 AS STRING))"
        - "TRIM(CAST(fonrc1 AS STRING))"
    ARTICLE_BK:
        - "TRIM(CAST(arnrc1 AS STRING))"
    SITE_BK:
        - "TRIM(CAST(finrc1 AS STRING))"
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
            - "arnrc1"
            - "askdc1"
            - "finrc1"
            - "foejc1"
            - "fokdc1"
            - "fonrc1"
            - "lvpfc1"
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
