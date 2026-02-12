{%- set yaml_metadata -%}
source_model: "stg_as400__purchase_invoice_status_omschrijving"
src_pk: "FIELD_VALUE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "field_value"
    - "field_name"
    - "field_value_description"
    - "file_name"
    - "file_description"
    - "last_change_user"
    - "last_change_timestamp"
src_extra_columns: "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
