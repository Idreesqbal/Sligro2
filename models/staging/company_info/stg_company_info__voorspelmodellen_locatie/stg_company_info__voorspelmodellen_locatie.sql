{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__voorspelmodellen_locatie'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__Voorspelmodellen_Locatie"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    EFFECTIVE_FROM: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    COMPANY_REFERENCE_BK: "SPLIT(sizolockey, '_')[SAFE_OFFSET(0)]"
    POSTCODE6: "{{ ci_sizolockey_postcode(6) }}"
    POSTCODE_BK: "{{ ci_sizolockey_postcode(4) }}"
    MASAT_LOCATION_PREDICTION: "!rv_masat__company_reference_location_predictions"
hashed_columns:
    COMPANY_REFERENCE_HK:
        - "COMPANY_REFERENCE_BK"
    POSTCODE_HK:
        - "POSTCODE_BK"
    COMPANY_REFERENCE_LOCATION_HK:
        - "COMPANY_REFERENCE_BK"
        - "POSTCODE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "sizolockey"
            - "sizcbgt"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
