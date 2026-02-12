/* Derived from the ingestion we have multiple rows in the Raw table
   with the same set of attributes, but different event_datetime */
WITH operational_info_data AS (
    SELECT
        article.articlecode,
        article.event_datetime,
        article.event_type,
        operationalinformation.locale,
        operationalinformation.scalesminimumshelflife,
        operationalinformation.scalesminimumshelflifeuom,
        operationalinformation.minimumshelflifeafterproduction,
        operationalinformation.minimumshelflifeafterproductionuom,
        operationalinformation.maximumstoragetemperature,
        operationalinformation.maximumstoragetemperatureuom
    FROM {{ source("data_hubs_to_dwh_landing_zone", 'raw_article_v1_1') }} AS article
    CROSS JOIN UNNEST(operationalinformation) AS operationalinformation
),

ranked_operational_info_events AS (
    SELECT
        articlecode,
        event_datetime,
        event_type,
        locale,
        scalesminimumshelflife,
        scalesminimumshelflifeuom,
        minimumshelflifeafterproduction,
        minimumshelflifeafterproductionuom,
        maximumstoragetemperature,
        maximumstoragetemperatureuom,
        ROW_NUMBER() OVER (
            PARTITION BY
                articlecode,
                event_type,
                locale,
                scalesminimumshelflife,
                scalesminimumshelflifeuom,
                minimumshelflifeafterproduction,
                minimumshelflifeafterproductionuom,
                maximumstoragetemperature,
                maximumstoragetemperatureuom
            ORDER BY event_datetime DESC
        ) AS rn
    FROM operational_info_data
)

SELECT
    articlecode,
    event_datetime,
    event_type,
    locale,
    scalesminimumshelflife,
    scalesminimumshelflifeuom,
    minimumshelflifeafterproduction,
    minimumshelflifeafterproductionuom,
    maximumstoragetemperature,
    maximumstoragetemperatureuom,
    -- Adding tracking columns for the satellite
    COALESCE(event_type LIKE '%create%', false) AS is_created,
    COALESCE(event_type LIKE '%deleted', false) AS is_deleted
FROM ranked_operational_info_events
WHERE rn = 1
-- This will only select the latest event for each article and operational information set
