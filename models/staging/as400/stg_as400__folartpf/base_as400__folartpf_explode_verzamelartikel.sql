-- If the article code in 'folartpf' is one that represents a "verzamelartikel".
-- Then, we can find the actual article codes in 'flvzflpf'.
-- If nothing is found in the latter than 'folartpf' already contains the correct article code
WITH promotion_collection_articles AS (
    SELECT *
    FROM {{ source('sligro60_slgfilelib_slgfilelib', 'folartpf') }}
),

promotion_actual_articles AS (
    SELECT *
    FROM {{ source('sligro60_slgfilelib_slgfilelib', 'flvzflpf') }}
)

SELECT
    promotion_collection_articles.*,
    coalesce(promotion_actual_articles.arn1ij, promotion_collection_articles.arnrgu) AS article_code
FROM promotion_collection_articles
LEFT JOIN promotion_actual_articles ON
    -- A unique article-promotion combination is defined by the (folder) promotion's
    -- year, number and code plus the article number
    promotion_collection_articles.fokdgu = promotion_actual_articles.fokdij
    AND promotion_collection_articles.foejgu = promotion_actual_articles.foejij
    AND promotion_collection_articles.fonrgu = promotion_actual_articles.fonrij
    AND promotion_collection_articles.arnrgu = promotion_actual_articles.arnrij
