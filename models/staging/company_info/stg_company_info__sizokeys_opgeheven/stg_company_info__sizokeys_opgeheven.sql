{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__sizokeys_opgeheven'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__SizoKeys_Opgeheven"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    COMPANY_REFERENCE_BK: "sizokey"
    SIZTOPHDAT: "PARSE_DATE('%Y%m%d', TRIM(CAST(siztophdat AS STRING)))"
    SAT_KEYS_DELETED: "!rv_sat__company_reference_company_keys_deleted"
hashed_columns:
    COMPANY_REFERENCE_HK:
        - "COMPANY_REFERENCE_BK"
    HASHDIFF:
      is_hashdiff: true
      columns:
        - "sizokey"
        - "selloph"
        - "selcoph"
        - "sizdoph"
        - "siztophdat"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
