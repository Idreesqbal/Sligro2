{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: folokcpf
derived_columns:
    RECORD_SOURCE: "!AS400__folokcpf"
    LOAD_DATETIME: "{{ fivetran_loaddate(["fokdc1", "foejc1", "fonrc1"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    PROMOTION_BK:
        - "TRIM(CAST(fokdc1 AS STRING))"
        - "TRIM(CAST(foejc1 AS STRING))"
        - "TRIM(CAST(fonrc1 AS STRING))"
hashed_columns:
    PROMOTION_HK:
        - "PROMOTION_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "foejc1"
            - "fokdc1"
            - "fonrc1"
            - "onalc1"
            - "onpbc1"
            - "opbdc1"
            - "opbtc1"
            - "opedc1"
            - "opetc1"
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
