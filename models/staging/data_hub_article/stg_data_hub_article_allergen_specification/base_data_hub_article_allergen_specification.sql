/* Derived from the ingestion we have multiple rows in the Raw table
   with the same set of attributes, but different event_datetime */
WITH allergenspecs AS (
    SELECT
        article.articlecode,
        article.event_datetime,
        article.event_type,
        allergenspecifications.locale,
        allergenspecifications.allergenstatement
    FROM {{ source("data_hubs_to_dwh_landing_zone", 'raw_article_v1_1') }} AS article
    CROSS JOIN UNNEST(allergenspecifications) AS allergenspecifications
),

ranked_allergen_events AS (
    SELECT
        articlecode,
        event_datetime,
        event_type,
        locale,
        allergenstatement,
        ROW_NUMBER() OVER (
            PARTITION BY
                articlecode,
                event_type,
                locale,
                allergenstatement
            ORDER BY event_datetime DESC
        ) AS rn
    FROM allergenspecs
)

SELECT
    articlecode,
    event_datetime,
    event_type,
    locale,
    allergenstatement,
    -- Adding tracking columns for the satellite
    COALESCE(event_type LIKE '%create%', false) AS is_created,
    COALESCE(event_type LIKE '%deleted', false) AS is_deleted
FROM ranked_allergen_events
WHERE rn = 1
-- This will only select the latest event for each unique article and allergen specification
