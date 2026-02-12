{%- set yaml_metadata -%}
source_model: "stg_as400__transporteur"
src_pk: "CARRIER_SITE_HK"
src_hashdiff: "CARRIER_OPERATIONAL_DETAILS_HASHDIFF"
src_payload:
    - "transporteur_id"
    - "transporteur_volgnummer"
    - "vestiging_id"
    - "transporteur_status"
    - "toegevoegd_door"
    - "gewijzigd_door"
    - "toegevoegd_datum"
    - "gewijzigd_datum"
    - "mutatie_teller"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
