WITH

hub_trade_item AS (
    SELECT
        trade_item_hk,
        trade_item_bk
    FROM {{ ref("rv_hub__trade_item") }}
),

latest_trade_item AS (
    SELECT
        trade_item_hk,
        max(load_datetime) AS max_load_datetime
    FROM {{ ref("rv_masat__trade_item_additional_gtin") }}
    WHERE is_deleted = false
    GROUP BY trade_item_hk
),


sat_trade_item_additional_gtin AS (
    SELECT
        sat.trade_item_hk,
        sat.main_gtin
    FROM {{ ref("rv_masat__trade_item_additional_gtin") }} AS sat
    INNER JOIN
        latest_trade_item AS la
        ON sat.trade_item_hk = la.trade_item_hk AND sat.load_datetime = la.max_load_datetime
    WHERE sat.is_deleted = false
),


result AS (
    SELECT
        hub.trade_item_bk AS pk_trade_item_additional_code,
        sat.main_gtin AS fk_trade_item_main_code
    FROM hub_trade_item AS hub
    INNER JOIN sat_trade_item_additional_gtin AS sat ON hub.trade_item_hk = sat.trade_item_hk
)

SELECT *
FROM result
