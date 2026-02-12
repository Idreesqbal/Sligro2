/* Derived from ingestion of the raw table with multiple rows
   having the same attributes except for event_datetime */
WITH trade_item_sales_area_data AS (
    SELECT
        tradeitems.gtin,
        event_datetime,
        event_type,
        salesarea.salesorganisationcode,
        salesarea.salesunitofmeasure,
        salesarea.salesdistributionchannelcode
    FROM {{ source("data_hubs_to_dwh_landing_zone", 'raw_article_v1_1') }}
    CROSS JOIN UNNEST(tradeitems) AS tradeitems
    CROSS JOIN UNNEST(tradeitems.salesareas) AS salesarea
),

ranked_trade_item_sales_area_events AS (
    SELECT
        gtin,
        event_datetime,
        event_type,
        salesorganisationcode,
        salesunitofmeasure,
        salesdistributionchannelcode,
        ROW_NUMBER() OVER (
            PARTITION BY
                gtin,
                event_type,
                salesorganisationcode,
                salesunitofmeasure,
                salesdistributionchannelcode
            ORDER BY event_datetime DESC
        ) AS rn
    FROM trade_item_sales_area_data
)

SELECT
    gtin,
    event_datetime,
    event_type,
    salesorganisationcode,
    salesunitofmeasure AS salesunit,
    salesdistributionchannelcode,
    -- Adding tracking columns for the satellite
    COALESCE(event_type LIKE '%create%', false) AS is_created,
    COALESCE(event_type LIKE '%deleted', false) AS is_deleted
FROM ranked_trade_item_sales_area_events
WHERE rn = 1
-- This will only select the latest event for each trade item sales area
