{%- set yaml_metadata -%}
source_model: "stg_as400__folprspf"
src_pk: "PROMOTION_ARTICLE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "aiprhv"
    - "arnrhv"
    - "cnprhv"
    - "foejhv"
    - "fokdhv"
    - "fonrhv"
    - "fopehv"
    - "foprhv"
    - "fxpehv"
    - "fypehv"
    - "kobdhv"
    - "kopchv"
    - "pejnhv"
    - "xcaahv"
    - "xcprhv"
    - "ycaahv"
    - "ycprhv"
src_extra_columns: "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
