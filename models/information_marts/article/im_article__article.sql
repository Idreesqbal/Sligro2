WITH

hub_article AS (
    SELECT
        article_hk,
        article_bk
    FROM {{ ref("rv_hub__article") }}
),

latest_articles AS (
    SELECT
        article_hk,
        max(load_datetime) AS max_load_datetime
    FROM {{ ref("rv_sat__article") }}
    WHERE is_deleted = false
    GROUP BY article_hk
),

sat_article AS (
    SELECT
        sat.article_hk,
        sat.sligrolegacybrandcode,
        sat.brandname AS brand_name,
        sat.scalesarticle AS scales_article_indicator,
        sat.plucode AS plu_code,
        sat.variableweightitem AS variable_weight_item_indicator,
        sat.orderunitofmeasure AS order_unit_of_measure,
        sat.baseunitofmeasure AS base_unit_of_measure,
        sat.articletype AS article_type,
        sat.issueunitofmeasure AS issue_unit_of_measure
    FROM {{ ref("rv_sat__article") }} AS sat
    INNER JOIN latest_articles AS la ON sat.article_hk = la.article_hk AND sat.load_datetime = la.max_load_datetime
    WHERE sat.is_deleted = false
),


result AS (
    SELECT
        hub.article_bk AS pk_article_code,
        sat.sligrolegacybrandcode AS fk_sligro_legacy_brand_code,
        sat.brand_name,
        sat.article_type,
        sat.scales_article_indicator,
        sat.plu_code,
        sat.variable_weight_item_indicator,
        sat.order_unit_of_measure,
        sat.base_unit_of_measure,
        sat.issue_unit_of_measure
    FROM hub_article AS hub
    INNER JOIN sat_article AS sat ON hub.article_hk = sat.article_hk
)

SELECT *
FROM result
