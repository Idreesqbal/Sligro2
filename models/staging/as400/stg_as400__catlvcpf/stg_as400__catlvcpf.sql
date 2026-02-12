{%- set yaml_metadata -%}
source_model:
    'base_as400__catlvcpf'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__catlvcpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SUPPLIER_BK:
        - "TRIM(CAST(leverancier_nummer AS STRING))"
    {# Technical changes to source columns #}
    categorie:
        - "TRIM(CAST(categorie AS STRING))"
hashed_columns:
    SUPPLIER_HK:
        - "SUPPLIER_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "leverancier_nummer"
            - "categorie"
            - "toegevoegd_door"
            - "toegevoegd_timestamp"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
