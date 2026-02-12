{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__locaties'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__Locaties"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    COMPANY_REFERENCE_BK: "sizokey_up"
    POSTCODE6: "{{ ci_sizolockey_postcode(6) }}"
    POSTCODE_BK: "{{ ci_sizolockey_postcode(4) }}"
    MASAT_LOCATION: "!rv_masat__company_reference_location"
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
            - "sizokey_up"
            - "sizosbil"
            - "sizosbi2l"
            - "sizosbisel"
            - "sizwam_loc"
            - "sizkam_loc"
            - "sizwai_loc"
            - "sizwlftl"
            - "sizklftl"
            - "sizwasbi2l"
            - "sizpmarbrl"
            - "sizkmarbrl"
            - "sizpclobal"
            - "sizkclobal"
            - "sizlondwel"
            - "sizlcustrl"
            - "gw_kwhitel"
            - "gw_kbluel"
            - "gw_kmobtll"
            - "gw_kjvgasl"
            - "gw_kjvelel"
            - "sellipl"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
