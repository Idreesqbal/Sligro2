{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__view_webdata_keywords_concern'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__View_Webdata_Keywords_Concern"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    COMPANY_REFERENCE_BK: "sizokey_up"
    SAT_KEYWORD: "!rv_sat__company_reference_webdata_keywords_ultimate_parent"
hashed_columns:
    COMPANY_REFERENCE_HK:
        - "COMPANY_REFERENCE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
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
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
