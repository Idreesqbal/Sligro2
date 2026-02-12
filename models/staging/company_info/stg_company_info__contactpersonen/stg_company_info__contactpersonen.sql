{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__contactpersonen'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__Contactpersonen"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    COMPANY_REFERENCE_BK: "sizokey"
    MASAT_CONTACT_PERSON: "!rv_masat__company_reference_contact_person"
    sizoptit: "NULLIF(TRIM(sizoptit), '')"
    sizoptvg: "NULLIF(TRIM(sizoptvg), '')"
hashed_columns:
    COMPANY_REFERENCE_HK:
        - "COMPANY_REFERENCE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "sizokey"
            - "sizocpkey"
            - "sizcpfnc"
            - "sizopsex"
            - "sizoptit"
            - "sizopvl"
            - "sizoptvg"
            - "sizopnm"
            - "sizoptvgnm"
            - "sizlpbrknd"
            - "sizlcp"
            - "sizcptbv"
            - "sizlpersgg"
            - "geboortedatum"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
