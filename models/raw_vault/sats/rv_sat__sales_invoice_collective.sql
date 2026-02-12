{%- set yaml_metadata -%}
source_model: "stg_as400__loufktpf"
src_pk: "SALES_INVOICE_HK"
src_hashdiff: "HASHDIFF"
src_payload:
    - "faktnr"
    - "filnr"
    - "factuurdatum"
    - "fakris"
    - "klanr"
    - "betkod"
    - "buisnr"
    - "embsal"
    - "inbkh"
    - "jnkdlt"
    - "minuut"
    - "netexc"
    - "netto"
    - "pcnr"
    - "specpr"
    - "srtfak"
    - "uur"
    - "vrzdat"
    - "vrzfak"
src_ldts: "LOAD_DATETIME"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(**metadata_dict) }}
