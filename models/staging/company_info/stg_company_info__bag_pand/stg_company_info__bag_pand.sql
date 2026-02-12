{%- set yaml_metadata -%}
source_model:
    'base_stg_company_info__bag_pand'
derived_columns:
    RECORD_SOURCE: "!COMPANY_INFO__BAG_Pand"
    LOAD_DATETIME: "TIMESTAMP(dt)"
    END_DATETIME: "CAST(NULL AS TIMESTAMP)"
    COMPANY_BUILDING_BK: "bagopndid"
    SAT_BUILDING: "!rv_sat__company_reference_building_details"
    PECWTUIOP: "SAFE_CAST(REPLACE(pecwtuiop, ',', '.') AS NUMERIC)"
hashed_columns:
    COMPANY_BUILDING_HK:
        - "COMPANY_BUILDING_BK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "bagopndid"
            - "sizcbedver"
            - "pecwozb"
            - "pecwpndih"
            - "peckpndih"
            - "pecwtuiop"
            - "pecwdakop"
            - "peckdakop"
            - "pecwpndho"
            - "peckpndho"
            - "peccdak"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(**metadata_dict) }}
