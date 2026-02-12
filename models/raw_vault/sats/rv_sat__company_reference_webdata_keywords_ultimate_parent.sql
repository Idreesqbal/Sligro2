{{ config(
    materialized='incremental',
    unique_key= ['COMPANY_REFERENCE_HK', 'LOAD_DATETIME']
) }}

{%- set yaml_metadata -%}
source_model: "stg_company_info__view_webdata_keywords_concern"
src_pk: "COMPANY_REFERENCE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "sizokey_up"
    - "wdilbetach"
    - "wdilfacebo"
    - "wdilinstgr"
    - "wdillinkin"
    - "wdilpinter"
    - "wdiltwitte"
    - "wdilthuisw"
    - "wdilyoutub"
    - "wdilfambed"
    - "wdilwebsho"
    - "wdildrzdrz"
    - "wdildrziso"
    - "wdildrzmvo"
    - "wdildrzspo"
    - "wdildrzmvs"
    - "wdilpakbez"
    - "wdilbtw"
    - "wdiliban"
    - "wdilmvopyr"
    - "wdiladword"
    - "wdilemail"
    - "wdilappand"
    - "wdilappapp"
    - "wdilappblb"
    - "wdilappwin"
    - "wdilconinf"
    - "wdilcontus"
    - "wdillogpag"
    - "wdilmobplf"
    - "wdilorgdat"
    - "wdilsersec"
src_extra_columns:
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ add_end_datetime_to_sat(metadata_dict) }}
