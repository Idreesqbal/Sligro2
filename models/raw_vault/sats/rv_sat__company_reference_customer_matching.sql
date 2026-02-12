{{ config(
    materialized='incremental',
    unique_key= ['COMPANY_REFERENCE_CUSTOMER_HK', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_company_info__matching_table"
src_pk:
    - "COMPANY_REFERENCE_CUSTOMER_HK"
src_hashdiff: "HASHDIFF"
src_payload:
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
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
