{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__branches'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__Branches"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    COMPANY_REFERENCE_BK: "sizokey"
    SAT_BUSINESS_ACTIVITY: "!rv_sat__company_reference_business_activity"
    kvk_hoofd_sbi_code: "NULLIF(TRIM(kvk_hoofd_sbi_code), '')"
    kvk_hoofd_sbi_code_omschrijving: "NULLIF(TRIM(kvk_hoofd_sbi_code_omschrijving), '')"
    kvk_neven_1_sbi_code: "NULLIF(TRIM(kvk_neven_1_sbi_code), '')"
    kvk_neven_1_sbi_omschrijving: "NULLIF(TRIM(kvk_neven_1_sbi_omschrijving), '')"
    kvk_neven_2_sbi_code: "NULLIF(TRIM(kvk_neven_2_sbi_code), '')"
    kvk_neven_2_sbi_omschrijving: "NULLIF(TRIM(kvk_neven_2_sbi_omschrijving), '')"
    kvk_neven_3_sbi_code: "NULLIF(TRIM(kvk_neven_3_sbi_code), '')"
    kvk_neven_3_sbi_omschrijving: "NULLIF(TRIM(kvk_neven_3_sbi_omschrijving), '')"
hashed_columns:
    COMPANY_REFERENCE_HK:
        - "COMPANY_REFERENCE_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "sizokey"
            - "kvk_hoofd_sbi_code"
            - "kvk_hoofd_sbi_code_omschrijving"
            - "kvk_neven_1_sbi_code"
            - "kvk_neven_1_sbi_omschrijving"
            - "kvk_neven_2_sbi_code"
            - "kvk_neven_2_sbi_omschrijving"
            - "kvk_neven_3_sbi_code"
            - "kvk_neven_3_sbi_omschrijving"
            - "company_info1_code"
            - "company_info1_omschrijving"
            - "company_info2_code"
            - "company_info2_omschrijving"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
