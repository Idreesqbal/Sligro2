{%- set yaml_metadata -%}
source_model:
    'base_as400__btw_codering'
derived_columns:
    {# Creating metadata and keys #}
    RECORD_SOURCE: "!AS400__btw_codering"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    BTW_BK:
        - "TRIM(CAST(btw_id AS STRING))"
    land_id:
        - "TRIM(CAST(land_id AS STRING))"
hashed_columns:
    BTW_HK:
        - "BTW_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "btw_id"
            - "gewijzigd_datum"
            - "gewijzigd_door"
            - "btw_oms"
            - "mutatie_teller"
            - "land_id"
            - "toegevoegd_datum"
            - "toegevoegd_door"
            - "_fivetran_start"
            - "_fivetran_end"
            - "_fivetran_active"
            - "_fivetran_deleted"
            - "_fivetran_synced"

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
