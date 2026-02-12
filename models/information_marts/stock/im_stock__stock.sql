WITH

sat__stock AS (
    SELECT
        *,
        totac0 AS totac
    FROM {{ ref ("rv_sat__stock_bs") }}
    UNION ALL
    SELECT
        *,
        totac1 AS totac
    FROM {{ ref ("rv_sat__stock_cdc") }}
),

source AS (
    SELECT
        sat__stock.start_datetime,
        hub__stock_location.stock_location_bk,
        hub__article.article_bk,
        sat__stock.pibuc1,
        sat__stock.thdec1,
        sat__stock.totac,
        hub__site.site_bk,
        hub__stock.stock_hk
    FROM {{ ref("rv_hub__stock") }} AS hub__stock
    INNER JOIN sat__stock
        ON hub__stock.stock_hk = sat__stock.stock_hk
    INNER JOIN {{ ref ("rv_lnk__stock__stock_location") }} AS lnk__stock__stock_location
        ON hub__stock.stock_hk = lnk__stock__stock_location.stock_hk
    INNER JOIN {{ ref ("rv_esat__stock__stock_location") }} AS esat__stock__stock_location
        ON
            lnk__stock__stock_location.stock_stock_location_hk = esat__stock__stock_location.stock_stock_location_hk
            AND sat__stock.start_datetime >= esat__stock__stock_location.start_datetime
            AND (
                sat__stock.start_datetime < esat__stock__stock_location.end_datetime
                OR esat__stock__stock_location.end_datetime IS null
            )
    INNER JOIN {{ ref ("rv_hub__stock_location") }} AS hub__stock_location
        ON lnk__stock__stock_location.stock_location_hk = hub__stock_location.stock_location_hk
    INNER JOIN {{ ref ("rv_lnk__site__collection_area__stock_location") }} AS lnk__site__collection_area__stock_location
        ON hub__stock_location.stock_location_hk = lnk__site__collection_area__stock_location.stock_location_hk
    INNER JOIN {{ ref ("rv_hub__site") }} AS hub__site
        ON lnk__site__collection_area__stock_location.site_hk = hub__site.site_hk
    INNER JOIN {{ ref ("rv_sat__site_details") }} AS sat__site_details
        ON hub__site.site_hk = sat__site_details.site_hk
    INNER JOIN {{ ref ("rv_lnk__article__stock") }} AS lnk__article__stock
        ON hub__stock.stock_hk = lnk__article__stock.stock_hk
    INNER JOIN {{ ref ("rv_hub__article_AS400") }} AS hub__article
        ON lnk__article__stock.article_hk = hub__article.article_hk
),

daily_updated AS (
    SELECT
        *,
        DATE(start_datetime) AS start_date,
        ROW_NUMBER() OVER (
            PARTITION BY
                stock_hk,
                DATE(start_datetime)
            ORDER BY start_datetime DESC
        ) AS rank_num
    FROM source
    QUALIFY rank_num = 1
),

stock_cdc_bs AS (
    SELECT
        stock_location_bk AS fk_stock_location_code,
        article_bk AS fk_article_code,
        null AS stock_weight,
        pibuc1 AS stock_type,
        CAST(null AS STRING) AS stock_type_description,
        'MISSING_UNKNOWN' AS brochure_indicator,
        'MISSING_UNKNOWN' AS owner,
        start_date AS `date`,
        SAFE.PARSE_DATE('%Y%m%d', CAST(thdec1 AS STRING)) AS tht_date,
        CONCAT(
            start_date, '||',
            stock_location_bk, '||',
            pibuc1, '||',
            article_bk, '||',
            thdec1
        ) AS pk_stock_code,
        SUM(totac) AS stock_quantity
    FROM daily_updated
    GROUP BY
        start_date,
        pibuc1,
        thdec1,
        stock_location_bk,
        article_bk,
        site_bk
),

-- ZB
sat_article_details AS (
    SELECT *
    FROM {{ ref("rv_sat__article_AS400_details") }}
    WHERE end_datetime IS null
),

link_article_location_supplier AS (
    SELECT
        article_hk,
        article_group_hk
    FROM {{ ref("rv_lnk__article__location__supplier") }}
    GROUP BY article_hk, article_group_hk
),

tlnk_stock_zb AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY article_hk, site_hk, DATE(load_datetime)
            ORDER BY CAST(fivetran_index AS INTEGER) DESC
        ) AS rank_num
    FROM {{ ref('rv_tlnk__stock_zb') }}
    QUALIFY rank_num = 1
),

-- The columns with empty strings or null values are placeholders.
-- The source for them couldn't be identified yet and should be added later.
stock_zb AS (
    SELECT
        tlnk_stock_zb.article_bk AS fk_article_code,
        tlnk_stock_zb.vranc1 AS stock_quantity,
        tlnk_stock_zb.vrkgc1 AS stock_weight,
        CAST(null AS STRING) AS stock_type,
        CAST(null AS STRING) AS stock_type_description,
        'MISSING_UNKNOWN' AS brochure_indicator,
        'MISSING_UNKNOWN' AS owner,
        CAST(null AS DATE) AS tht_date,
        DATE(tlnk_stock_zb.load_datetime) AS `date`,
        CONCAT(
            DATE(tlnk_stock_zb.load_datetime), '||',
            tlnk_stock_zb.site_bk,
            '||', hub_article_group.article_group_nk, '||', sat_article_details.vrklc1, '||',
            tlnk_stock_zb.article_bk
        ) AS pk_stock_code,
        CONCAT(tlnk_stock_zb.site_bk, '||', hub_article_group.article_group_nk, '||', sat_article_details.vrklc1)
            AS fk_stock_location_code
    FROM tlnk_stock_zb
    INNER JOIN {{ ref('rv_hub__article_AS400') }} AS hub_article
        ON tlnk_stock_zb.article_hk = hub_article.article_hk
    INNER JOIN sat_article_details
        ON hub_article.article_hk = sat_article_details.article_hk
    INNER JOIN link_article_location_supplier
        ON hub_article.article_hk = link_article_location_supplier.article_hk
    INNER JOIN {{ ref("rv_hub__article_group") }} AS hub_article_group
        ON link_article_location_supplier.article_group_hk = hub_article_group.article_group_hk
    WHERE sat_article_details.vrklc1 != 0
),

unioning AS (
    {% set list_of_columns = [
        'pk_stock_code',
        'fk_stock_location_code',
        'fk_article_code',
        'stock_quantity',
        'stock_weight',
        'stock_type',
        'stock_type_description',
        'brochure_indicator',
        'tht_date',
        'owner',
        'date'
    ] -%}
    {% set list_of_ctes = ['stock_cdc_bs', 'stock_zb'] -%}
    {% for cte in list_of_ctes %}
        SELECT {{ list_of_columns | join(', ') }}
        FROM {{ cte }}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {%- endfor %}
)

SELECT *
FROM unioning
