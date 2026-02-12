WITH

sat_details_cdc AS (
    SELECT *
    FROM {{ ref("rv_masat__stock_location_details_cdc") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY stock_location_hk, vpomc1 ORDER BY load_datetime DESC) = 1
),

stock_location_cdc AS (
    SELECT
        -- hub_stock_location
        hub_stock_location.stock_location_bk AS pk_stock_location_code,
        -- hub_collection_area
        hub_collection_area.collection_area_nk AS fk_collection_area_code,
        -- sat_details_cdc
        CAST(sat_details_cdc.pgnrc1 AS STRING) AS aisle,
        null AS point_of_sale, -- empty for cdc
        CAST(null AS STRING) AS shelf_layout,
        CAST(null AS STRING) AS container_type,
        CAST(null AS STRING) AS capacity,
        CAST(null AS BOOLEAN) AS sprinkler_indicator,
        CAST(null AS BOOLEAN) AS sort_to_light_indicator,
        CAST(null AS STRING) AS shelf,
        CAST(sat_details_cdc.pvnrc2 AS STRING) AS compartment,
        CAST(sat_details_cdc.plnrc2 AS STRING) AS layer,
        CAST(sat_details_cdc.ppnrc2 AS STRING) AS location,
        CONCAT(
            sat_details_cdc.pgnrc1,
            '.',
            sat_details_cdc.pvnrc2,
            '.',
            sat_details_cdc.plnrc2,
            '.',
            sat_details_cdc.ppnrc2
        ) AS location_number,
        STRING_AGG(DISTINCT sat_details_cdc.vpomc1, ', ' ORDER BY sat_details_cdc.vpomc1) AS unit
    FROM {{ ref("rv_hub__stock_location") }} AS hub_stock_location
    INNER JOIN sat_details_cdc
        ON hub_stock_location.stock_location_hk = sat_details_cdc.stock_location_hk
    INNER JOIN {{ ref("rv_lnk__article__location__supplier") }} AS link_article__location__supplier
        ON hub_stock_location.stock_location_hk = link_article__location__supplier.stock_location_hk
    INNER JOIN {{ ref("rv_hub__collection_area") }} AS hub_collection_area
        ON link_article__location__supplier.collection_area_hk = hub_collection_area.collection_area_hk
    GROUP BY
        hub_stock_location.stock_location_bk,
        hub_collection_area.collection_area_nk,
        sat_details_cdc.pgnrc1,
        sat_details_cdc.pvnrc2,
        sat_details_cdc.plnrc2,
        sat_details_cdc.ppnrc2
),

sat_details_bs AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY stock_location_hk, vpomc1
            ORDER BY load_datetime DESC
        ) AS check_rank
    FROM {{ ref("rv_masat__stock_location_details_bs") }}
    QUALIFY check_rank = 1
),

stock_location_bs AS (
    SELECT
        -- hub_stock_location
        hub_stock_location.stock_location_bk AS pk_stock_location_code,

        -- hub_collection_area
        hub_collection_area.collection_area_nk AS fk_collection_area_code,

        -- sat_details_bs
        sat_details_bs.pgnrc1 AS aisle,
        CAST(null AS STRING) AS shelf, -- empty for bs
        CASE WHEN sat_details_bs.plnrc2 != '' THEN sat_details_bs.plnrc2 END AS layer,
        null AS point_of_sale,
        CAST(null AS STRING) AS shelf_layout,
        CAST(null AS STRING) AS container_type,
        CAST(null AS STRING) AS capacity,
        CAST(null AS BOOLEAN) AS sprinkler_indicator,
        CAST(null AS BOOLEAN) AS sort_to_light_indicator,
        CAST(sat_details_bs.pvnrc1 AS STRING) AS compartment,
        CAST(sat_details_bs.ppnrc1 AS STRING) AS location,
        CONCAT(
            sat_details_bs.pgnrc1,
            '.',
            sat_details_bs.pvnrc1,
            '.',
            sat_details_bs.plnrc2,
            '.',
            sat_details_bs.ppnrc1
        ) AS location_number,
        STRING_AGG(DISTINCT TRIM(sat_details_bs.vpomc1), ', ' ORDER BY TRIM(sat_details_bs.vpomc1)) AS unit
    FROM {{ ref("rv_hub__stock_location") }} AS hub_stock_location
    INNER JOIN sat_details_bs
        ON hub_stock_location.stock_location_hk = sat_details_bs.stock_location_hk
    INNER JOIN {{ ref("rv_lnk__article__location__supplier") }} AS link_article__location__supplier
        ON hub_stock_location.stock_location_hk = link_article__location__supplier.stock_location_hk
    INNER JOIN {{ ref("rv_hub__collection_area") }} AS hub_collection_area
        ON link_article__location__supplier.collection_area_hk = hub_collection_area.collection_area_hk
    GROUP BY
        hub_stock_location.stock_location_bk,
        hub_collection_area.collection_area_nk,
        sat_details_bs.pgnrc1,
        sat_details_bs.pvnrc1,
        sat_details_bs.plnrc2,
        sat_details_bs.ppnrc1
),

-- ZB stock location is the same for all ZBs, it is a combination of Article Group and
-- “display order”. We can discard the ones with “display order” = 0
sat_article_details AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY article_hk
            ORDER BY load_datetime DESC
        ) AS check_rank
    FROM {{ ref("rv_sat__article_AS400_details") }}
    QUALIFY check_rank = 1
),

link_article_location_supplier AS (
    SELECT
        article_hk,
        article_group_hk,
        site_hk
    FROM {{ ref("rv_lnk__article__location__supplier") }}
    GROUP BY article_hk, article_group_hk, site_hk
),

stock_location_zb AS (
    SELECT
        CAST(null AS STRING) AS fk_collection_area_code,
        CAST(null AS STRING) AS aisle, -- empty for zb
        CAST(null AS STRING) AS shelf, -- empty for zb
        CAST(null AS STRING) AS compartment, -- empty for zb
        CAST(null AS STRING) AS layer, -- empty for zb
        CAST(null AS STRING) AS location, -- empty for zb
        sat_article_details.vrklc1 AS point_of_sale,
        CAST(null AS STRING) AS shelf_layout,
        CAST(null AS STRING) AS container_type,
        CAST(null AS STRING) AS capacity,
        CAST(null AS BOOLEAN) AS sprinkler_indicator,
        CAST(null AS BOOLEAN) AS sort_to_light_indicator,
        CAST(null AS STRING) AS location_number,
        CONCAT(hub_site.site_bk, '||', hub_article_group.article_group_nk, '||', sat_article_details.vrklc1)
            AS pk_stock_location_code,
        STRING_AGG(DISTINCT TRIM(sat_article_details.vpomc1), ', ' ORDER BY TRIM(sat_article_details.vpomc1)) AS unit
    FROM {{ ref("rv_hub__article_AS400") }} AS hub_article
    INNER JOIN sat_article_details
        ON hub_article.article_hk = sat_article_details.article_hk
    INNER JOIN link_article_location_supplier
        ON hub_article.article_hk = link_article_location_supplier.article_hk
    INNER JOIN {{ ref("rv_hub__article_group") }} AS hub_article_group
        ON link_article_location_supplier.article_group_hk = hub_article_group.article_group_hk
    INNER JOIN {{ ref("rv_hub__site") }} AS hub_site
        ON link_article_location_supplier.site_hk = hub_site.site_hk
    WHERE sat_article_details.vrklc1 != 0
    GROUP BY pk_stock_location_code, sat_article_details.vrklc1
),

unioning AS (
    {% set list_of_columns = [
        'pk_stock_location_code',
        'fk_collection_area_code',
        'aisle',
        'shelf',
        'compartment',
        'layer',
        'location',
        'point_of_sale',
        'location_number',
        'shelf_layout',
        'container_type',
        'capacity',
        'sprinkler_indicator',
        'sort_to_light_indicator',
        'unit'
    ] -%}
    {% set list_of_ctes = ['stock_location_cdc', 'stock_location_bs', 'stock_location_zb'] -%}
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
