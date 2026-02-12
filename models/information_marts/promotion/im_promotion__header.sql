WITH

hub_promotion AS (
    SELECT
        promotion_hk,
        promotion_bk
    FROM {{ ref("rv_hub__promotion") }}
),

promotion_header AS (
    SELECT
        promotion_hk,
        fokdgw AS code,
        foejgw AS year,
        fonrgw AS number,
        oms3gw AS description,
        uzkdgw AS sales_channel,
        zvdegw AS start_date_cash_and_carry,
        zedegw AS end_date_cash_and_carry,
        bvdegw AS start_date_delivery,
        bedegw AS end_date_delivery,
        vndegw AS start_date_deal_period,
        tmdegw AS end_date_deal_period,
        swkdgw AS status,
        badegw AS concept_date,
        akdegw AS final_date
    FROM {{ ref("rv_sat__promotion_header") }}
    WHERE end_datetime IS null
),

promotion_header_central AS (
    SELECT
        promotion_hk,
        kenmerk_waarde AS country
    FROM {{ ref("rv_sat__promotion_header_central") }}
    {# The source table contains a variety of metadata about the promotion
    in long format. When Kenmerk_code = 'FOLDERLAND' it means that the
    describing attribute ('kenmerk_waarde') will contain the country in
    which the promotion is valid. #}
    WHERE TRIM(kenmerk_code) = 'FOLDERLAND' AND end_datetime IS null
),

promotion_header_online AS (
    SELECT
        promotion_hk,
        onpbc1 AS published_online,
        {{ cast_to_datetime('opbdc1', 'opbtc1') }}
            AS start_date_published_online,
        {{ cast_to_datetime('opedc1', 'opetc1') }}
            AS end_date_published_online
    FROM {{ ref("rv_sat__promotion_header_online") }}
    WHERE end_datetime IS null
),


result AS (
    SELECT
        promotion_header.* EXCEPT (promotion_hk),
        promotion_header_online.* EXCEPT (promotion_hk),
        hub.promotion_bk AS promotion_code,
        promotion_header_central.country
    FROM hub_promotion AS hub
    LEFT JOIN promotion_header
        ON hub.promotion_hk = promotion_header.promotion_hk
    LEFT JOIN promotion_header_online
        ON hub.promotion_hk = promotion_header_online.promotion_hk
    LEFT JOIN promotion_header_central
        ON hub.promotion_hk = promotion_header_central.promotion_hk
)

SELECT *
FROM result
