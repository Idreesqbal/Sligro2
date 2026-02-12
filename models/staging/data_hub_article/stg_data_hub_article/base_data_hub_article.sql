/* Derived from the ingestion we have multiple rows in the Raw table
   with the same set of attributes, but different event_datetime */
WITH rankedevents AS (
    SELECT
        articlecode,
        event_datetime,
        event_type,
        sligrolegacybrandcode,
        brandname,
        scalesarticle,
        plucode,
        variableweightitem,
        orderunitofmeasure,
        baseunitofmeasure,
        articletype,
        issueunitofmeasure,
        ROW_NUMBER() OVER (
            PARTITION BY
                articlecode,
                event_type,
                sligrolegacybrandcode,
                brandname,
                scalesarticle,
                plucode,
                variableweightitem,
                orderunitofmeasure,
                baseunitofmeasure,
                articletype,
                issueunitofmeasure
            ORDER BY event_datetime DESC
        ) AS rn
    FROM {{ source("data_hubs_to_dwh_landing_zone", 'raw_article_v1_1') }}
)

SELECT
    articlecode,
    event_datetime,
    event_type,
    sligrolegacybrandcode,
    brandname,
    scalesarticle,
    plucode,
    variableweightitem,
    orderunitofmeasure,
    baseunitofmeasure,
    articletype,
    issueunitofmeasure,
    -- Adding tracking columns for the satellite
    COALESCE(event_type LIKE '%create%', false) AS is_created,
    COALESCE(event_type LIKE '%deleted', false) AS is_deleted
FROM rankedevents
WHERE rn = 1  -- This will only select the latest event for each unique set of attributes
