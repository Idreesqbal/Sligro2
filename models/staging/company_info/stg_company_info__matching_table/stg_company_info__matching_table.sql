{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__matching_table'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__MatchingTable"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    COMPANY_REFERENCE_BK: "sizokey"
    klantkey: "(CAST(klantkey AS STRING))"
    CUSTOMER_BK: "(CAST(klantkey AS STRING))"
    DATUM_OPGEHEVEN: "PARSE_DATE('%d-%m-%Y', DATUM_OPGEHEVEN)"
    SAT_CUSTOMER_MATCH: "!rv_sat__company_reference_customer_matching"
hashed_columns:
    COMPANY_REFERENCE_HK:
        - "COMPANY_REFERENCE_BK"
    CUSTOMER_HK:
        - "CUSTOMER_BK"
    COMPANY_REFERENCE_CUSTOMER_HK:
        - "COMPANY_REFERENCE_BK"
        - "CUSTOMER_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
        - "klantkey"
        - "sizokey"
        - "opgeheven_jn"
        - "datum_opgeheven"
        - "naam_score"
        - "adres_score"
        - "kvk_score"
        - "telefoon_score"
        - "opgehevend_score"
        - "totaal_score"
        - "startdatum_klant"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
