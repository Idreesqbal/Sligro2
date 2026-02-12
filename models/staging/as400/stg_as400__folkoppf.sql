{%- set yaml_metadata -%}
source_model:
    sligro60_slgmutlib_slgfilelib: folkoppf
derived_columns:
    RECORD_SOURCE: "!AS400__folkoppf"
    LOAD_DATETIME: "{{ fivetran_loaddate(["fokdgw", "foejgw", "fonrgw"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    PROMOTION_BK:
        - "TRIM(CAST(fokdgw AS STRING))"
        - "TRIM(CAST(foejgw AS STRING))"
        - "TRIM(CAST(fonrgw AS STRING))"
hashed_columns:
    PROMOTION_HK:
        - "PROMOTION_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
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
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(
      include_source_columns=true,
      source_model=metadata_dict['source_model'],
      derived_columns=metadata_dict['derived_columns'],
      null_columns=none,
      hashed_columns=metadata_dict['hashed_columns'],
      ranked_columns=none
  ) }}
