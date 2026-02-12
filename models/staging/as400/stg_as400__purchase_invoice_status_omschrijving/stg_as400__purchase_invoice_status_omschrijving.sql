{%- set yaml_metadata -%}
source_model:
    'base_as400__purchase_invoice_status_omschrijving'

derived_columns:
    RECORD_SOURCE: "!AS400__status_omschrijving"
    LOAD_DATETIME: "_fivetran_synced"
    EFFECTIVE_FROM: "_fivetran_start"
    START_DATETIME: "_fivetran_start"
    END_DATETIME: "{{ fivetran_end() }}"
    FIELD_VALUE_BK:
      - "TRIM(CAST(field_value AS STRING))"

hashed_columns:
    FIELD_VALUE_HK:
      - "FIELD_VALUE_BK"

    HASHDIFF:
      is_hashdiff: true
      columns:
        - "file_description"
        - "field_name"
        - "field_value_description"
        - "field_description"
        - "last_change_user"
        - "last_change_timestamp"
        - "_fivetran_start"
        - "_fivetran_end"
        - "_fivetran_active"
        - "_fivetran_synced"
        - "_fivetran_deleted"
        - "_fivetran_id"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
