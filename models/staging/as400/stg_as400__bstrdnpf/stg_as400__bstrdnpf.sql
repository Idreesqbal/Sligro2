{%- set yaml_metadata -%}
source_model:
  'base_as400__bstrdnpf'

derived_columns:
  RECORD_SOURCE: "!AS400__bstrdnpf"
  LOAD_DATETIME: "{{ fivetran_last_touched() }}"
  IONRBR_BK:
    - "TRIM(CAST(IONRBR AS STRING))"
  IRNRBR_BK:
    - "TRIM(CAST(IRNRBR AS STRING))"

hashed_columns:
  BSTRDNPF_HK:
    - "IONRBR_BK"
    - "IRNRBR_BK"

  HASHDIFF:
    is_hashdiff: true
    columns:
      - "ORIRBR"
      - "ARGRBR"
      - "RDKDBR"
      - "RDOMBR"
      - "EHAABR"
      - "EHPRBR"
      - "OSKDBR"
      - "GRFDBR"
      - "KPFDBR"
      - "KDFDBR"
      - "GRNFBR"
      - "KPNFBR"
      - "KDNFBR"
      - "HTDTBR"
      - "HTUSBR"
      - "LAKDBR"
      - "MSFDBR"
      - "MSNFBR"
      - "_fivetran_synced"
      - "_fivetran_deleted"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
