{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__bag'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__BAG"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    COMPANY_ADDRESS_BK: "sizolocidm"
    COMPANY_BUILDING_BK: "bagopndid"
    SAT_ADDRESS: "!rv_sat__company_reference_address_details"
    SIZOLATIT: "SAFE_CAST(REPLACE(SIZOLATIT, ',', '.') AS NUMERIC)"
    SIZOLONGIT: "SAFE_CAST(REPLACE(SIZOLONGIT, ',', '.') AS NUMERIC)"
hashed_columns:
    COMPANY_ADDRESS_HK:
        - "COMPANY_ADDRESS_BK"
    COMPANY_BUILDING_HK:
        - "COMPANY_BUILDING_BK"
    COMPANY_BUILDING_ADDRESS_HK:
        - "COMPANY_ADDRESS_BK"
        - "COMPANY_BUILDING_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "sizolocidm"
            - "bagonumid"
            - "bagoobrid"
            - "bagopltid"
            - "bagovboid"
            - "bagoligid"
            - "bagostdid"
            - "bagopndid"
            - "bagcnumtyp"
            - "bagopcd"
            - "bagohnr"
            - "bagothn"
            - "bagokix"
            - "bagwbwjr"
            - "bagkbwjr"
            - "bagwopvl"
            - "bagkopvl"
            - "bagcgbdl"
            - "baglgd1bij"
            - "baglgd2cel"
            - "baglgd3gez"
            - "baglgd4ind"
            - "baglgd5kan"
            - "baglgd6log"
            - "baglgd7ond"
            - "baglgd8spo"
            - "baglgd9win"
            - "baglgd10wo"
            - "baglgd0ove"
            - "bagcstpnd"
            - "bagcstvbo"
            - "sizolatit"
            - "sizolongit"
            - "sizordx"
            - "sizordy"
            - "sizogeo"
            - "sizogeotxt"
            - "sizcloctyp"
            - "sizwconadr"
            - "sizkconadr"
            - "pectypwo"
            - "pecceig"
            - "pecwwoz"
            - "peccenerl"
            - "peccorient"
            - "cienerl"
            - "cieninx"
            - "cienest"
            - "bagladrhfd"
            - "bagrbesnum"
            - "bagostr"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
