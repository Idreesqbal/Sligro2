{#
    Company Info classifies areas into a commercial/industrial estate name based on 6-digit postcode.
    For the related satelites to form the data of this mart,
    we filter out the deleted data by calling out the ci_filter_deleted_rows macros.
#}
WITH
active_commercial_area_keys AS (
    {{ ci_filter_deleted_rows(
        rv_hub_link = ref('rv_hub__postcode'),
        rv_xts = ref('rv_xts__company_reference_bedrijfsterreinen'),
        pk = 'postcode_hk',
        tracked_sat_name = 'rv_masat__postcode_commercial_area'
        ) }}
),

sat_commercial_area AS (
    SELECT *
    FROM {{ ref("rv_masat__postcode_commercial_area") }}
    WHERE end_datetime IS null
),

result AS (
    SELECT
        CONCAT(sat_commercial_area.sizopcd, '||', sat_commercial_area.sizoplannm)
            AS pk_company_reference_commercial_area_code,
        sat_commercial_area.sizopcd AS postal_code_6_digit,
        active_commercial_area_keys.postcode_bk AS postal_code_4_digit,
        sat_commercial_area.sizoplannm AS industrial_estate_name,
        sat_commercial_area.sizokernnm AS industrial_estate_group_name
    FROM active_commercial_area_keys
    INNER JOIN sat_commercial_area
        ON
            active_commercial_area_keys.postcode_hk = sat_commercial_area.postcode_hk
            AND active_commercial_area_keys.hashdiff = sat_commercial_area.hashdiff
)

SELECT *
FROM result
