/* Derived from the ingestion we have multiple rows in the Raw table
   with the same set of attributes, but different event_datetime */
WITH instructions_data AS (
    SELECT
        article.articlecode,
        article.event_datetime,
        article.event_type,
        instructions.locale,
        instructions.preparationinstructions,
        instructions.defrostinstructions,
        instructions.consumerusageinstructions,
        instructions.consumerstorageinstructions
    FROM {{ source("data_hubs_to_dwh_landing_zone", 'raw_article_v1_1') }} AS article
    CROSS JOIN UNNEST(instructions) AS instructions
),

ranked_instructions_events AS (
    SELECT
        articlecode,
        event_datetime,
        event_type,
        locale,
        preparationinstructions,
        defrostinstructions,
        consumerusageinstructions,
        consumerstorageinstructions,
        ROW_NUMBER() OVER (
            PARTITION BY
                articlecode,
                event_type,
                locale,
                preparationinstructions,
                defrostinstructions,
                consumerusageinstructions,
                consumerstorageinstructions
            ORDER BY event_datetime DESC
        ) AS rn
    FROM instructions_data
)

SELECT
    articlecode,
    event_datetime,
    event_type,
    locale,
    preparationinstructions,
    defrostinstructions,
    consumerusageinstructions,
    consumerstorageinstructions,
    -- Adding tracking columns for the satellite
    COALESCE(event_type LIKE '%create%', false) AS is_created,
    COALESCE(event_type LIKE '%deleted', false) AS is_deleted
FROM ranked_instructions_events
WHERE rn = 1
-- This will only select the latest event for each article and instruction set
