{%- set yaml_metadata -%}
source_model:
  gcs_manual_imports: promotion_discount_mechanism_mapping
derived_columns:
  RECORD_SOURCE: "!GCS_MANUAL_IMPORTS__promotion_discount_mechanism_mapping"
  LOAD_DATETIME: current_timestamp()
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(
      include_source_columns=true,
      source_model=metadata_dict['source_model'],
      derived_columns=metadata_dict['derived_columns'],
      null_columns=none,
      ranked_columns=none
  ) }}
