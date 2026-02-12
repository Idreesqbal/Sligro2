{%- set yaml_metadata -%}
source_model: "stg_as400__btw_codering"
src_pk:           "BTW_HK"
src_hashdiff:     "HASHDIFF"
src_payload:
  - "land_id"
  - "btw_oms"
  - "toegevoegd_datum"
  - "toegevoegd_door"
  - "gewijzigd_datum"
  - "gewijzigd_door"
src_extra_columns:
  - "_fivetran_start"
  - "_fivetran_end"
  - "_fivetran_active"
  - "_fivetran_synced"
src_ldts:         "LOAD_DATETIME"
src_source:       "RECORD_SOURCE"
{%- endset -%}

{% set md = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(
    source_model     = md.source_model,
    src_pk            = md.src_pk,
    src_hashdiff      = md.src_hashdiff,
    src_payload       = md.src_payload,
    src_extra_columns = md.src_extra_columns,
    src_ldts          = md.src_ldts,
    src_source        = md.src_source
) }}
