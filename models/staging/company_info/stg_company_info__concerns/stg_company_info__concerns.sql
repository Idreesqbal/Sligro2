{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__concerns'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__Concerns"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    COMPANY_REFERENCE_BK: "sizokey_up"
    SAT_CONCERNS: "!rv_sat__company_reference_concern_details"
hashed_columns:
    COMPANY_REFERENCE_HK:
        - "COMPANY_REFERENCE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
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
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
