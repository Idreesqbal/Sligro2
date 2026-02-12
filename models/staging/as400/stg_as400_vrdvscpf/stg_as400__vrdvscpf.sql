{%- set yaml_metadata -%}
source_model:
    'base_as400__vrdvscpf'
derived_columns:
    RECORD_SOURCE: "!AS400__vrdvscpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    ARTICLE_BK:
        - "TRIM(CAST(ARNRC1 AS STRING))"
    SITE_BK:
        - "TRIM(CAST(FINRC1 AS STRING))"
    FIVETRAN_INDEX:
        - "{{ fivetran_index(["ARNRC1", "FINRC1", "CYDXC1"]) }}"
hashed_columns:
    ARTICLE_HK:
        - "ARTICLE_BK"
    SITE_HK:
        - "SITE_BK"
    ARTICLE_SITE_STOCK_HK:
        - "_fivetran_id"
        - "ARTICLE_BK"
        - "SITE_BK"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
