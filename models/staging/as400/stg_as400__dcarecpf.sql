{%- set yaml_metadata -%}
source_model:
    sligro60_slgfilelib_slgfilelib: dcarecpf
derived_columns:
    RECORD_SOURCE: "!AS400__dcarecpf"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SITE_NK:
        - "!{{ var("cdc_sitecode") }}" # HR.001
    COLLECTION_AREA_NK:
        - "!{{ var("cdc_sitecode") }}" # HR.001
        - "TRIM(CAST(DCKDC1 AS STRING))"
        - "TRIM(CAST(AREAC1 AS STRING))"
    DAOMC1:
        - "TRIM(CAST(DAOMC1 AS STRING))"
    DCKDC1:
        - "TRIM(CAST(DCKDC1 AS STRING))"
    FVJNC1:
        - "TRIM(CAST(FVJNC1 AS STRING))"
hashed_columns:
    SITE_HK:
        - "SITE_NK"
    COLLECTION_AREA_HK:
        - "COLLECTION_AREA_NK"
    COLLECTION_AREA_SITE_HK:
        - "COLLECTION_AREA_NK"
        - "SITE_NK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "COLLECTION_AREA_NK"
            - "DCKDC1"
            - "AREAC1"
            - "COURC1"
            - "AFCOC1"
            - "AFVOC1"
            - "DAOMC1"
            - "AKKDC1"
            - "APKDC1"
            - "USRXC1"
            - "PLJNC1"
            - "PLDRC1"
            - "PIJNC1"
            - "RFKDC1"
            - "OLJNC1"
            - "LWJNC1"
            - "SLJNC1"
            - "PLVRC1"
            - "PLZTC1"
            - "PLRSC1"
            - "FVJNC1"
            - "FVDRC1"
            - "FVVRC1"
            - "BPSCC1"
            - "PLBLC1"
            - "MCAVC1"
            - "PLSRC2"
            - "PLACC1"
            - "AIPCC1"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
