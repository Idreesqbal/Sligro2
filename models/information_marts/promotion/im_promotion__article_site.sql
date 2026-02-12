WITH

lnk_promotion_article AS (
    SELECT
        promotion_article_hk,
        promotion_hk,
        article_hk
    FROM {{ ref("rv_lnk__promotion_article") }}
    WHERE end_datetime IS null
),


hub_promotion AS (
    SELECT
        promotion_hk,
        promotion_bk
    FROM {{ ref("rv_hub__promotion") }}
),

hub_article AS (
    SELECT
        article_hk,
        article_bk
    FROM {{ ref("rv_hub__article_AS400") }}
),

sat_promotion_article_concept_site AS (
    SELECT
        promotion_article_hk,
        finrc1 AS site_bk
    FROM {{ ref("rv_sat__promotion_article_concept_site") }}
    WHERE
        TRIM(askdc1) = 'J' {# Indicator that article is in assortment at site #}
        AND end_datetime IS null
),

sat_promotion_article_definitive_site AS (
    SELECT
        promotion_article_hk,
        finrgx AS site_bk
    FROM {{ ref("rv_sat__promotion_article_definitive_site") }}
    WHERE
        TRIM(askdgx) = 'J'
        AND end_datetime IS null
),

result AS (
    SELECT
        hub_prom.promotion_bk AS promotion_code,
        hub_art.article_bk AS article_code,
        COALESCE(sat_pa_art_ds.site_bk, sat_pa_art_cs.site_bk) AS site_code
    FROM lnk_promotion_article AS lnk
    LEFT JOIN hub_promotion AS hub_prom
        ON lnk.promotion_hk = hub_prom.promotion_hk
    LEFT JOIN hub_article AS hub_art
        ON lnk.article_hk = hub_art.article_hk
    LEFT JOIN sat_promotion_article_definitive_site AS sat_pa_art_ds
        ON lnk.promotion_article_hk = sat_pa_art_ds.promotion_article_hk
    {# Only join concept sites to rows which have no definitive site to prevent duplicates. #}
    LEFT JOIN sat_promotion_article_concept_site AS sat_pa_art_cs
        ON
            lnk.promotion_article_hk = sat_pa_art_cs.promotion_article_hk
            AND sat_pa_art_ds.site_bk IS null
    {# To retain a list of only valid promotion-article & site combinations
    the NULL rows are filtered out as those represent promotion articles
    that are not available at any site (i.e. no entry in concept/definitive site) #}
    WHERE COALESCE(sat_pa_art_ds.site_bk, sat_pa_art_cs.site_bk) IS NOT null
)

SELECT *
FROM result
