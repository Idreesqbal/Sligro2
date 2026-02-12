{{
    config(
        enabled = target.name != 'prod'
    )
}}

WITH

hub_article AS (
    SELECT
        article_hk,
        article_bk
    FROM {{ ref("rv_hub__article") }}
),

latest_articles AS (
    SELECT
        sat.article_hk,
        hub.article_bk,
        max(sat.load_datetime) AS max_load_datetime
    FROM {{ ref("rv_sat__article") }} AS sat
    INNER JOIN hub_article AS hub ON sat.article_hk = hub.article_hk
    WHERE sat.is_deleted = false
    GROUP BY
        sat.article_hk,
        hub.article_bk
),

-- Get the latest active rpe_supplier data filtered by Article
max_ts_article AS (
    SELECT
        article_id,
        max(load_datetime) AS max_load_datetime
    FROM {{ ref("rv_sat__article__data_supplier") }}
    GROUP BY article_id
),

latest_rpe_supplier AS (
    SELECT sat.*
    FROM {{ ref("rv_sat__article__data_supplier") }} AS sat
    INNER JOIN max_ts_article AS latest
        ON sat.article_id = latest.article_id AND sat.load_datetime = latest.max_load_datetime
),

sat_rpe_supplier AS (
    SELECT
        sat.rpe_supplier_hk,
        concat(sat.article_id, '|', coalesce(sat.rpe_supplier_bk, 'NULL')) AS pk_rpe_supplier_code,
        sat.article_id,
        sat.article_ownership_nl,
        sat.article_ownership_be,
        sat.unit,
        sat.supplier_id,
        sat.supplier_name,
        sat.supplier_gln
    FROM latest_rpe_supplier AS sat
    LEFT JOIN
        latest_articles
            AS la
        ON
            sat.article_id = la.article_bk
),

result AS (
    SELECT
        sat.pk_rpe_supplier_code AS pk_data_supplier_code,
        sat.article_id AS fk_article_code,
        sat.article_ownership_nl AS ownership_nl,
        sat.article_ownership_be AS ownership_be,
        sat.unit,
        sat.supplier_id AS data_supplier_id,
        sat.supplier_name AS data_supplier_name,
        sat.supplier_gln AS data_supplier_gln
    FROM sat_rpe_supplier AS sat
)

SELECT * FROM result
