SELECT
    article.articlecode,
    article.event_datetime,
    article.event_type,
    t.gtin,
    t.unitofmeasure,
    t.grossweight,
    t.grossweightuom,
    t.packagingdepth,
    t.netweight,
    t.netweightuom,
    t.drainedweight,
    t.drainedweightuom,
    t.packagingdepthuom,
    t.packagingwidth,
    t.packagingwidthuom,
    t.packagingheight,
    t.packagingheightuom,
    t.netcontents,
    t.netcontentsuom,
    t.numberofbaseunits,
    t.legacyarticlenumbersligrom,
    -- Deleted in Articl v1_1
    -- t.legacyarticlenumberjava,
    t.legacyarticlenumbersligro,
    t.legacyarticlenumberaantafel,
    t.legacyarticlenumbersligroispc,
    t.additionalgtins,
    -- Adding tracking columns for the satellite
    COALESCE(event_type LIKE '%create%', false) AS is_created,
    COALESCE(event_type LIKE '%deleted', false) AS is_deleted
FROM {{ source("data_hubs_to_dwh_landing_zone", 'raw_article_v1_1') }} AS article
CROSS JOIN UNNEST(tradeitems) AS t
