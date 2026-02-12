/* Derived from the ingestion we have multiple rows in the Raw table
   with the same set of attributes, but different event_datetime */

WITH additional_gtin_data AS (
    SELECT
        tradeitems.gtin AS main_gtin,
        event_datetime,
        event_type,
        additionalgtins AS additional_gtin
    FROM {{ source("data_hubs_to_dwh_landing_zone", 'raw_article_v1_1') }}
    CROSS JOIN UNNEST(tradeitems) AS tradeitems
    CROSS JOIN UNNEST(tradeitems.additionalgtins) AS additionalgtins
),

ranked_additional_gtin_events AS (
    SELECT
        main_gtin,
        event_datetime,
        event_type,
        additional_gtin,
        ROW_NUMBER() OVER (
            PARTITION BY
                event_type,
                main_gtin,
                additional_gtin
            ORDER BY event_datetime DESC
        ) AS rn
    FROM additional_gtin_data
)

SELECT
    main_gtin,
    additional_gtin,
    event_datetime,
    event_type,
    -- Adding tracking columns for the satellite
    COALESCE(event_type LIKE '%create%', false) AS is_created,
    COALESCE(event_type LIKE '%deleted', false) AS is_deleted
FROM ranked_additional_gtin_events
WHERE
    rn = 1
-- This will only select the latest event for each main GTIN and additional GTIN combination
