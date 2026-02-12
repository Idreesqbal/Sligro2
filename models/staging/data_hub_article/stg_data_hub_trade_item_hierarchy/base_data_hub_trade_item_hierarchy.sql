/* Derived from ingestion of the raw table with multiple rows
   having the same attributes except for event_datetime */
WITH trade_item_hierarchy_data AS (
    SELECT
        tradeitems.gtin,
        event_datetime,
        event_type,
        tradeitemhierarchy.quantityoflowerlevel,
        tradeitemhierarchy.childgtin
    FROM {{ source("data_hubs_to_dwh_landing_zone", 'raw_article_v1_1') }}
    CROSS JOIN UNNEST(tradeitems) AS tradeitems
    CROSS JOIN UNNEST(tradeitems.tradeitemhierarchies) AS tradeitemhierarchy
),

ranked_trade_item_hierarchy_events AS (
    SELECT
        gtin,
        event_datetime,
        event_type,
        quantityoflowerlevel,
        childgtin,
        ROW_NUMBER() OVER (
            PARTITION BY
                gtin,
                event_type,
                quantityoflowerlevel,
                childgtin
            ORDER BY event_datetime DESC
        ) AS rn
    FROM trade_item_hierarchy_data
)

SELECT
    gtin,
    quantityoflowerlevel,
    childgtin,
    event_datetime,
    event_type,
    -- Adding tracking columns for the satellite
    COALESCE(event_type LIKE '%create%', false) AS is_created,
    COALESCE(event_type LIKE '%deleted', false) AS is_deleted
FROM ranked_trade_item_hierarchy_events
WHERE rn = 1
-- This will only select the latest event for each trade item hierarchy
