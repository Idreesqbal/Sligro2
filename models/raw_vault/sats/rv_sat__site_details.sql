{%- set yaml_metadata -%}
source_model: "stg_as400__filialpf"
src_pk: "SITE_HK"
src_hashdiff:
    source_column: "HASHDIFF"
    alias: "HASHDIFF"
src_payload:
    - adrsc1
    - fin1c1
    - finrc1
    - finrc4
    - finsc1
    - fisrc1
    - fltyc1
    - fxnrc1
    - ifjnc1
    - naamc1
    - nfm1c1
    - nfm2c1
    - nfm3c1
    - nft1c1
    - pfnrc1
    - primc1
    - pritc1
    - pskdc1
    - retvc1
    - rpjnc1
    - s006c1
    - sejnc1
    - srk1c1
    - srk2c1
    - srk3c1
    - srk4c1
    - srk5c1
    - tfnrc1
    - tiosc1
    - vn04c1
    - vn10c1
    - von1c1
    - von2c1
    - von3c1
    - von4c1
    - von5c1
    - wnplc1
    - zmkdc1
    - "START_DATETIME"
    - "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
