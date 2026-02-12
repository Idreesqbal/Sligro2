{%- set yaml_metadata -%}
source_model:
    'base_as400__lvedicpf'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__lvedicpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SUPPLIER_BK:
        - "TRIM(CAST(lvnrc1 AS STRING))"
    {# Technical changes to source columns #}
    vrobc1:
        - "TRIM(CAST(vrobc1 AS STRING))"
hashed_columns:
    SUPPLIER_HK:
        - "SUPPLIER_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "lvnrc1"
            - "bstmc1"
            - "vrobc1"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
