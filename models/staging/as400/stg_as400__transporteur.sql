-- When a supplier is assigned as a carrier, it uses the same BK value.
{%- set yaml_metadata -%}
source_model:
    sligro60_slgmutlib_boordcomp: transporteur
derived_columns:
    RECORD_SOURCE: "!AS400__transporteur"
    LOAD_DATETIME: "{{ fivetran_loaddate(['transporteur_id']) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    CARRIER_BK:
        - "TRIM(CAST(transporteur_id AS STRING))"
    SUPPLIER_BK:
        - "TRIM(CAST(transporteur_id AS STRING))"
    SITE_BK:
        - "TRIM(CAST(vestiging_id AS STRING))"
hashed_columns:
    CARRIER_HK:
        - "CARRIER_BK"
    SUPPLIER_HK:
        - "SUPPLIER_BK"
    SITE_HK:
        - "SITE_BK"
    SUPPLIER_CARRIER_HK:
        - "SUPPLIER_BK"
        - "CARRIER_BK"
    CARRIER_SITE_HK:
        - "CARRIER_BK"
        - "SITE_BK"
    CARRIER_OPERATIONAL_DETAILS_HASHDIFF:
        is_hashdiff: true
        columns:
            - "transporteur_id"
            - "transporteur_volgnummer"
            - "vestiging_id"
            - "transporteur_status"
            - "toegevoegd_door"
            - "gewijzigd_door"
            - "toegevoegd_datum"
            - "gewijzigd_datum"
            - "mutatie_teller"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
