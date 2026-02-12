{{ config(
    materialized='incremental',
    unique_key= ['COMPANY_REFERENCE_HK', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_company_info__concerns"
src_pk: "COMPANY_REFERENCE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "sizokey_up"
    - "sizosbic"
    - "sizosbi2c"
    - "sizosbisec"
    - "sizwam_co"
    - "sizkam_co"
    - "sizwai_co"
    - "sizwloc_co"
    - "sizwlftc"
    - "sizklftc"
    - "sizwasbi2c"
    - "sizpmarbrc"
    - "sizpclobac"
    - "sizlondwec"
    - "sizlcustrc"
    - "sizlintc"
    - "sellbrvc"
    - "sizccomint"
    - "sizcafzgeb"
    - "sizcwebprc"
    - "sizcsocmec"
    - "sizcmvopyc"
    - "sizlinterc"
    - "sizkinttec"
    - "sizkintlnc"
    - "sizlsisite"
    - "sizwcodiep"
    - "sizkloc_co"
    - "sizkai_co"
    - "sizcmkb"
    - "gw_kwhitec"
    - "gw_kbluec"
    - "gw_kmobtlc"
    - "trdkam_co"
    - "sellipc"
    - "cinldrzcir"
    - "cincdrzcir"
    - "selllnki"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
