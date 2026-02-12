{%- set yaml_metadata -%}
source_model:
  gcs_manual_imports: company_reference_SBI_category_segment_mapping
derived_columns:
  RECORD_SOURCE: "!GCS_MANUAL_IMPORTS__company_reference_SBI_category_segment_mapping"
  LOAD_DATETIME: current_timestamp()
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
