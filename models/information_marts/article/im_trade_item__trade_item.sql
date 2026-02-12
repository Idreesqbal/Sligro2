WITH

hub_trade_item AS (
    SELECT
        trade_item_hk,
        trade_item_bk
    FROM {{ ref("rv_hub__trade_item") }}
),


lnk_article__trade_item AS (
    SELECT
        trade_item_hk,
        article_hk
    FROM {{ ref('rv_lnk__article__trade_item') }}
),


hub_article AS (
    SELECT
        article_hk,
        CAST(article_bk AS STRING) AS article_bk
    FROM {{ ref("rv_hub__article") }}
),

latest_trade_items AS (
    SELECT
        trade_item_hk,
        MAX(load_datetime) AS max_load_datetime
    FROM {{ ref("rv_sat__trade_item") }}
    WHERE is_deleted = false
    GROUP BY trade_item_hk
),


sat_trade_item AS (
    SELECT
        sat.trade_item_hk,
        sat.legacyarticlenumbersligrom AS legacy_article_number_sligro_m,
        sat.legacyarticlenumbersligro AS legacy_article_number_sligro,
        sat.legacyarticlenumberaantafel AS legacy_article_number_aan_tafel,
        sat.legacyarticlenumbersligroispc AS legacy_article_number_sligro_ispc,
        sat.unitofmeasure AS unit_of_measure,
        sat.grossweight AS gross_weight,
        sat.grossweightuom AS gross_weight_uom,
        sat.netweight AS net_weight,
        sat.netweightuom AS net_weight_uom,
        sat.drainedweight AS drained_weight,
        sat.drainedweightuom AS drained_weight_uom,
        sat.netcontents AS net_contents,
        sat.packagingdepth AS packaging_depth,
        sat.packagingdepthuom AS packaging_depth_uom,
        sat.packagingwidth AS packaging_width,
        sat.packagingwidthuom AS packaging_width_uom,
        sat.packagingheight AS packaging_height,
        sat.packagingheightuom AS packaging_height_uom,
        sat.netcontentsuom AS net_contents_uom,
        sat.numberofbaseunits AS number_of_base_units
        -- Removed in Article V1_1
        -- sat.legacyarticlenumberjava
    FROM {{ ref("rv_sat__trade_item") }} AS sat
    INNER JOIN
        latest_trade_items AS lti
        ON sat.trade_item_hk = lti.trade_item_hk AND sat.load_datetime = lti.max_load_datetime
    WHERE sat.is_deleted = false
),


result AS (
    SELECT
        hub_trade_item.trade_item_bk AS pk_trade_item_code,
        hub_article.article_bk AS fk_article_code,
        sat.number_of_base_units,
        sat.unit_of_measure,
        sat.packaging_width,
        sat.packaging_width_uom,
        sat.packaging_height,
        sat.packaging_height_uom,
        sat.packaging_depth,
        sat.packaging_depth_uom,
        sat.net_weight,
        sat.net_weight_uom,
        sat.drained_weight,
        sat.drained_weight_uom,
        sat.gross_weight,
        sat.gross_weight_uom,
        sat.net_contents,
        sat.net_contents_uom,
        sat.legacy_article_number_sligro_m,
        -- Removed in Article v1_1
        --sat.legacy_article_number_java,
        sat.legacy_article_number_sligro,
        sat.legacy_article_number_aan_tafel,
        sat.legacy_article_number_sligro_ispc
    FROM sat_trade_item AS sat
    LEFT JOIN lnk_article__trade_item AS lnk ON sat.trade_item_hk = lnk.trade_item_hk
    LEFT JOIN hub_article ON lnk.article_hk = hub_article.article_hk
    LEFT JOIN hub_trade_item ON sat.trade_item_hk = hub_trade_item.trade_item_hk

)

SELECT *
FROM result
