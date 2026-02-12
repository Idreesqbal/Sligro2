WITH

hub_trade_item AS (
    SELECT
        trade_item_hk,
        trade_item_bk
    FROM {{ ref("rv_hub__trade_item") }}
),


hlnk_trade_item_hierarchy AS (
    SELECT
        trade_item_hk,
        trade_item_child_hk
    FROM {{ ref('rv_hlnk__trade_item_hierarchy') }}
),


latest_trade_item AS (
    SELECT
        trade_item_hk,
        MAX(load_datetime) AS max_load_datetime
    FROM {{ ref("rv_sat__trade_item_hierarchy") }}
    WHERE is_deleted = false
    GROUP BY trade_item_hk
),

sat_trade_item_hierarchy AS (
    SELECT
        sat.trade_item_hk,
        sat.quantityoflowerlevel
    FROM {{ ref("rv_sat__trade_item_hierarchy") }} AS sat
    INNER JOIN latest_trade_item AS lti
        ON
            sat.trade_item_hk = lti.trade_item_hk
            AND sat.load_datetime = lti.max_load_datetime
    WHERE sat.is_deleted = false
),

result AS (
    SELECT
        hub_parent.trade_item_bk AS fk_trade_item_parent_code,
        hub_child.trade_item_bk AS fk_trade_item_child_code,
        sat.quantityoflowerlevel AS quantity_of_lower_level,
        CONCAT(hub_parent.trade_item_bk, "||", hub_child.trade_item_bk) AS pk_trade_item_hierarchy_code
    FROM hlnk_trade_item_hierarchy AS lnk
    INNER JOIN hub_trade_item AS hub_child ON lnk.trade_item_child_hk = hub_child.trade_item_hk
    LEFT JOIN hub_trade_item AS hub_parent ON lnk.trade_item_hk = hub_parent.trade_item_hk
    LEFT JOIN sat_trade_item_hierarchy AS sat ON lnk.trade_item_hk = sat.trade_item_hk
)


SELECT DISTINCT
    pk_trade_item_hierarchy_code,
    fk_trade_item_parent_code,
    fk_trade_item_child_code,
    quantity_of_lower_level
FROM result
