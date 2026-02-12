{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__labels'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__Labels"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    SIZWLAB: "CAST(sizwlab AS STRING)"
    SIZTLAB: "NULLIF(TRIM(SIZTLAB), '')"
    SIZTLABENG: "NULLIF(TRIM(SIZTLABENG), '')"
    COMPANY_REFERENCE_LABELS_BK:
        - "identifier"
        - "veldnaam"
        - "sizwlab"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
