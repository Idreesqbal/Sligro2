WITH unnested_products AS (
    SELECT
        a.*,
        p.id AS article_id,
        p.articleownership_nl AS article_ownership_nl,
        p.articleownership_be AS article_ownership_be,
        p.unit
    FROM {{ source("step_to_dwh_landing_zone", 'raw_article_data_quality') }} AS a,
        UNNEST(a.products) AS p
),

data_supplier AS (
    SELECT
        up.*,
        rs.supplierid,
        rs.suppliername,
        rs.suppliergln
    FROM unnested_products AS up
    -- Adding a left join to ensure that, when the list of suppliers is empty it is treated as a delete
    LEFT JOIN UNNEST(up.products[OFFSET(0)].rpe_supplier) AS rs ON true
),

ranked_data AS (
    SELECT
        *,
        ROW_NUMBER()
            OVER (
                PARTITION BY
                    article_id, article_ownership_nl, article_ownership_be, unit, supplierid, suppliername, suppliergln
                ORDER BY publish_time DESC
            )
            AS rn
    FROM
        data_supplier
),

result AS (
    SELECT
        article_id,
        article_ownership_nl,
        article_ownership_be,
        unit,
        supplierid AS supplier_id,
        suppliername AS supplier_name,
        suppliergln AS supplier_gln,
        subscription_name,
        message_id,
        publish_time
    FROM ranked_data
    WHERE rn = 1
)

SELECT * FROM result
