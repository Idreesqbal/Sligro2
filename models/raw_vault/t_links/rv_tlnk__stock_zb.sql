{%- set yaml_metadata -%}
source_model: "stg_as400__vrdvscpf"
src_pk: "ARTICLE_SITE_STOCK_HK"
src_fk:
    - "ARTICLE_HK"
    - "SITE_HK"
src_payload:
    - "_fivetran_id"
    - "ARTICLE_BK"
    - "SITE_BK"
    - "VRANC1"
    - "VRKGC1"
    - "VRAOC1"
    - "GLAAC1"
    - "FIVETRAN_INDEX"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.t_link(**metadata_dict) }}
