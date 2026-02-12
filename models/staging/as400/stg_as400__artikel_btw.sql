{%- set yaml_metadata -%}
source_model:
    sligro60_slgmutlib_centraal: artikel_btw
derived_columns:
    RECORD_SOURCE: "!AS400__artikel_btw"
    LOAD_DATETIME: "{{ fivetran_loaddate(["artikel_id"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    ARTICLE_BK:
        - "TRIM(CAST(artikel_id AS STRING))"
hashed_columns:
    ARTICLE_HK:
        - "ARTICLE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "artikel_id"
            - "land_id"
            - "btw_id"
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
