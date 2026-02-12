{%- set yaml_metadata -%}
source_model:
    sligro60_slgmutlib_leverdatum: verzamel_gebied_vestiging
derived_columns:
    RECORD_SOURCE: "!AS400__verzamel_gebied_vestiging"
    LOAD_DATETIME: "{{ fivetran_last_touched() }}"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    SITE_NK:
        - "TRIM(CAST(VESTIGING_ID AS STRING))"
    COLLECTION_AREA_NK:
        - "TRIM(CAST(VESTIGING_ID AS STRING))"
        - "TRIM(CAST(VERZAMEL_GEBIED_ID AS STRING))"
    FOLDER_VERDELING:
        - "TRIM(CAST(FOLDER_VERDELING AS STRING))"
    VERZAMEL_GEBIED_ID:
        - "TRIM(CAST(VERZAMEL_GEBIED_ID AS STRING))"
    VERZAMEL_GEBIED_OMS:
        - "TRIM(CAST(VERZAMEL_GEBIED_OMS AS STRING))"
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
            - "VESTIGING_ID"
            - "VERZAMEL_GEBIED_ID"
            - "VERZAMEL_GEBIED_OMS_KORT"
            - "VERZAMEL_GEBIED_OMS"
            - "AFSCHERMEN_ARTIKEL"
            - "VERZAMEL_GEBIED_IS_FOLDER_STRAAT"
            - "SAMEN_VOEG_CODE_AFDRUKKEN_AFLEVER_BON"
            - "UITSLUITEN_EXTRA_KOSTEN_MENU_KAART"
            - "TRANSPORT_CONDITIE"
            - "AANVUL_VERZOEKEN"
            - "BULK_NAAR_PICK_SCANNEN"
            - "VERZAMEL_GEBIED_CODE"
            - "GO_DIRECT_NAAR_LOCATIE_BIJ_LABEL"
            - "VOLUME_PERCENTAGE_INTERTOUR"
            - "VOLUME_BEREKENEN_KRATTEN"
            - "CONTROLE_BIJ_INNAME"
            - "EMBALLAGE_NR"
            - "SSCC_VANAF"
            - "SSCC_TOT_EN_MET"
            - "SAMEN_VOEGEN_OP_SSCC"
            - "SAMEN_VOEG_CODE_VERDICHTEN_EMBALLAGE"
            - "ORDER_TIJD"
            - "AFKAPPEN_VOLUME_ROL_CONTAINER"
            - "AFKAP_PER_STUK_ROL_CONTAINER"
            - "LABELS_ROL_CONTAINER"
            - "PICK_LIJST_OP_KLANT_NIVEAU"
            - "PICK_LIJST_AUTOMATISCH_VRIJGEVEN"
            - "VERZAMEL_CODE"
            - "MONO_PALLETS_ORDERS_AFSPLITSEN"
            - "MONO_PALLET_VERZAMEL_GEBIED"
            - "CONSUMENTEN_ADVIES_PRIJS_AFDRUKKEN"
            - "ORDER_NUMMER_OP_STICKER"
            - "KLANT_NUMMER_AFKORTING_AFDRUKKEN"
            - "CODE_LOCATIE"
            - "TIJDEN_VERZAMELAAR_REGISTREREN"
            - "COLLI_PER_UUR_VERZAMELEN"
            - "VOLUME_PER_KRAT"
            - "AANTAL_LABELS"
            - "VERVOER_LABEL_VOORAF_AFDRUKKEN"
            - "SOORT_PICK_LIJST"
            - "OUTQUEUE_PICK_LIJST"
            - "OUTQUEUE_VERVOER_LABEL"
            - "SOORT_TRANSPORT_LABEL"
            - "SCANNING_ACTIEF"
            - "BACK_ORDER_BIJ_SCANNING"
            - "OUTQUEUE_SCANNING"
            - "VERVOER_LABEL_VERZAMEL_EENHEID"
            - "FOLDER_VERDELING"
            - "MAP_FOLDER_VERDELING"
            - "VERSIE_FOLDER_VERDELING"
            - "SOORT_PTL"
            - "MAP_PICK_TO_LIGHT"
            - "PICK_TO_LIGHT_VERSIE"
            - "OUTQUEUE_PTL"
            - "PTL_ACTIEF"
            - "AUTO_AANVULVZ_MANCOS_PTL"
            - "AUTOMATISCH_INNEMEN"
            - "BACK_ORDER_BIJ_AUTO_INNAME"
            - "ALTERNATIEF_VERZAMEL_GEBIED"
            - "VOLUME"
            - "TOTAAL_GEWICHT"
            - "AANTAL_COLLI"
            - "ORDER_AFKAPPEN_COLLI_AANTAL"
            - "TOEGEVOEGD_DOOR"
            - "TOEGEVOEGD_DATUM"
            - "_fivetran_start"
            - "_fivetran_end"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
