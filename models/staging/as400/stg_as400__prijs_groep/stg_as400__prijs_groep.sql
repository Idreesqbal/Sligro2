{%- set yaml_metadata -%}
source_model:
    'base_as400__prijs_groep'
derived_columns:
    RECORD_SOURCE: "!AS400__prijs_groep"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    PRICE_GROUP_BK:
        - "TRIM(CAST(PRIJS_GROEP_ID AS STRING))"
    SITE_BK:
        - "TRIM(CAST(HOOFD_KANTOOR_ID AS STRING))"

hashed_columns:
    PRICE_GROUP_HK:
        - "PRICE_GROUP_BK"
    SITE_HK:
        - "SITE_BK"

    HASHDIFF:
        is_hashdiff: true
        columns:
            - "CALCULATIE_AFRONDING"
            - "CALCULATIE_CONSUMENT_AFRONDING"
            - "CONSUMENT_PRIJS_BASIS_PRIJS_GROEP_ID"
            - "CONSUMENT_PRIJS_LEIDEND"
            - "CONSUMENT_PRIJS_PRIJS_CODE_OPSLAG_PERCENTAGE"
            - "CONSUMENT_PRIJS_VAST_OPSLAG_PERCENTAGE"
            - "HOOFD_KANTOOR_ID"
            - "INGANG_NIEUW_PRIJS"
            - "KLANT_SPECIFIEK"
            - "NUL_PRIJS_TOESTAAN"
            - "PRIJS_BASIS"
            - "PRIJS_BEVESTIGEN"
            - "PRIJS_GROEP_ID"
            - "PRIJS_GROEP_OMS"
            - "PRIJS_GROEP_SOORT"
            - "TERMIJN_PRIJS"
            - "VAST_PRIJS"
            - "VOLGORDE"
            - "_fivetran_start"
            - "_fivetran_end"

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
