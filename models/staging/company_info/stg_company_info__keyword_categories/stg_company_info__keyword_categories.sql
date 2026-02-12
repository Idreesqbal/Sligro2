{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__keyword_categories'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__Keyword_Categories"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    COMPANY_REFERENCE_BK: "sizokey"
    POSTCODE6: "{{ ci_sizolockey_postcode(6) }}"
    klantkey: "COALESCE(klantkey, ' ')"
    sizolockey: "COALESCE(sizolockey, ' ')"
    MASAT_KEYWORD_CATEGORIES: "!rv_masat__company_reference_keyword_categories"
hashed_columns:
    COMPANY_REFERENCE_HK:
        - "COMPANY_REFERENCE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "klantkey"
            - "sizokey"
            - "sizolockey"
            - "afhalen"
            - "bezorgen"
            - "dark_kitchen"
            - "platformen"
            - "terras"
            - "uitsluiten"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
