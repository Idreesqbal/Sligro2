/* Derived from ingestion of the raw table with multiple rows
   having the same attributes except for event_datetime */
WITH type_specific_attributes_data AS (
    SELECT
        article.articlecode,
        article.event_datetime,
        article.event_type,
        typespecificattributes.locale,
        typespecificattributes.name,
        typespecificattributes.value
    FROM {{ source("data_hubs_to_dwh_landing_zone", 'raw_article_v1_1') }} AS article
    CROSS JOIN UNNEST(typespecificattributes) AS typespecificattributes
),

ranked_type_specific_attributes_events AS (
    SELECT
        articlecode,
        event_datetime,
        event_type,
        locale,
        name,
        value,
        ROW_NUMBER() OVER (
            PARTITION BY
                articlecode,
                event_type,
                locale,
                name,
                value
            ORDER BY event_datetime DESC
        ) AS rn
    FROM type_specific_attributes_data
)

SELECT
    articlecode,
    event_datetime,
    event_type,
    locale,
    name,
    value,
    -- Adding tracking columns for the satellite
    COALESCE(event_type LIKE '%create%', false) AS is_created,
    COALESCE(event_type LIKE '%deleted', false) AS is_deleted
FROM ranked_type_specific_attributes_events
WHERE rn = 1
-- This will only select the latest event for each article type-specific attribute
