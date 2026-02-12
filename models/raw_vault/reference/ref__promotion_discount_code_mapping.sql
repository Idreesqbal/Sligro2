{%- set yaml_metadata -%}
source_model: "stg_gcs_manual_imports__promotion_discount_mechanism_mapping"
src_pk: "Code"
src_extra_columns: "Omschrijving"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ref_table(**metadata_dict) }}
