{%- set yaml_metadata -%}
source_model: "stg_as400__folkoppf"
src_pk: "PROMOTION_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "aadegw"
    - "akdegw"
    - "badegw"
    - "bedegw"
    - "bsdegw"
    - "btdegw"
    - "bvdegw"
    - "cedegw"
    - "crdegw"
    - "cvdegw"
    - "dedegw"
    - "eidegw"
    - "fejngw"
    - "foejgw"
    - "fojngw"
    - "fokdgw"
    - "fonrgw"
    - "oms3gw"
    - "sckdgw"
    - "swkdgw"
    - "tmdegw"
    - "uzkdgw"
    - "vndegw"
    - "zedegw"
    - "zvdegw"
src_extra_columns: "END_DATETIME"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
