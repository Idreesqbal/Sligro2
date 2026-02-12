{%- set yaml_metadata -%}
source_model: "stg_gcs_manual_imports__company_SBI_category_segment_mapping"
src_pk: "sbi_code"
src_extra_columns:
    - "sbi_description"
    - "category_code"
    - "category_description"
    - "segment_code"
    - "segment_description"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ref_table(**metadata_dict) }}
