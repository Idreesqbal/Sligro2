{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__bedrijfsterreinen'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__Bedrijfsterreinen"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    POSTCODE_BK: "CAST(LEFT(sizopcd, 4) AS STRING)"
    sizopcd: "REPLACE(sizopcd, ' ', '')"
    MASAT_COMMERCIAL_AREA: "!rv_masat__postcode_commercial_area"
hashed_columns:
    POSTCODE_HK:
        - "POSTCODE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "sizopcd"
            - "sizoplannm"
            - "sizokernnm"
            - "sizobtrvnr"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
