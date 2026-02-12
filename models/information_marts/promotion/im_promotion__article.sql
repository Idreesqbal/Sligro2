WITH

lnk_promotion_article AS (
    SELECT
        promotion_article_hk,
        promotion_hk,
        article_hk
    FROM {{ ref("rv_lnk__promotion_article") }}
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

sat_promotion_article AS (
    SELECT
        promotion_article_hk,
        zgaagu AS stamp_quantity,
        pfnrgu AS page,
        lcnrgu AS section_number
    FROM {{ ref("rv_sat__promotion_article") }}
    WHERE end_datetime IS null
),

sat_promotion_article_expected_sales_volume AS (
    SELECT
        promotion_article_hk,
        {# Articles can occur multiple times in the same promotion under a different "verzamelartikel"
        However, this distinction is not relevant and we want to aggregate it to the promo-article level #}
        SUM(aanthy) AS expected_sales_volume
    FROM {{ ref("rv_sat__promotion_article_expected_sales_volume") }}
    WHERE end_datetime IS null
    GROUP BY promotion_article_hk
),


sat_promotion_article_discount_mechanism_code AS (
    SELECT
        promotion_article_hk,
        TRIM(tx70gt) AS discount_mechanism_code
    FROM {{ ref("rv_sat__promotion_article_discount_mechanism") }}
    {# The source table contains a variety of metadata about the promotion
    article in long format. When rgnrgt (regelnummer) = 94 it means that
    the describing attribute (tx70gt) will contain the code that describes
    which discount mechanism is applied. #}
    WHERE rgnrgt = 94 AND end_datetime IS null
),

ref_promotion_article_discount_mechanism_description AS (
    SELECT
        {# The code consists of 2 digits and 1 letter where the letter is irrelevant #}
        omschrijving AS discount_mechanism_description,
        LEFT(code, 2) AS discount_mechanism_code
    FROM {{ ref("ref__promotion_discount_code_mapping") }}
),

sat_promotion_article_discount_mechanism_value AS (
    SELECT
        promotion_article_hk,
        tx70gt AS discount_mechanism_value
    FROM {{ ref("rv_sat__promotion_article_discount_mechanism") }}
    {# The source table contains a variety of metadata about the promotion
    article in long format. When rgnrgt (regelnummer) = 90 it means that
    the describing attribute (tx70gt) will contain the value related to the
    applied discount mechanism (e.g. '20' if discount code '%' is chosen
     and a 20% discount is applied) #}
    WHERE rgnrgt = 90 AND end_datetime IS null
),

sat_promotion_article_price AS (
    SELECT
        promotion_article_hk,
        foprhv AS promotion_price,
        xcaahv AS x_colli_quantity,
        xcprhv AS x_colli_price,
        ycaahv AS y_colli_quantity,
        ycprhv AS y_colli_price,
        fxpehv AS x_colli_price_per_unit,
        fypehv AS y_colli_price_per_unit
    FROM {{ ref("rv_sat__promotion_article_price") }}
    WHERE end_datetime IS null
),

sat_promotion_country AS (
    SELECT
        promotion_hk,
        TRIM(kenmerk_waarde) AS country
    FROM {{ ref("rv_sat__promotion_header_central") }}
    {# The source table contains a variety of metadata about the promotion
    in long format. When Kenmerk_code = 'FOLDERLAND' it means that the
    describing attribute ('kenmerk_waarde') will contain the country in
    which the promotion is valid. #}
    WHERE TRIM(kenmerk_code) = 'FOLDERLAND' AND end_datetime IS null
),

sat_article_purchase_price AS (
    SELECT
        article_hk,
        actueel_prijs AS purchase_price,
        {{ headquarter_to_country('hoofd_kantoor_id') }} AS country
    FROM {{ ref("rv_sat__article_AS400_price") }}
    WHERE
        calculatie_prijs_soort_id = 3.0 AND end_datetime IS null
        AND hoofd_kantoor_id IN (5000.0, 5180)
),

sat_article_cash_and_carry_price AS (
    SELECT
        article_hk,
        actueel_prijs AS cash_and_carry_price,
        {{ headquarter_to_country('hoofd_kantoor_id') }} AS country
    FROM {{ ref("rv_sat__article_AS400_price") }}
    WHERE
        calculatie_prijs_soort_id = 4.0 AND end_datetime IS null
        AND hoofd_kantoor_id IN (5000.0, 5180)
),

sat_article_vat AS (
    SELECT
        article_hk,
        btw_id AS vat_code,
        TRIM(land_id) AS country
    FROM {{ ref("rv_sat__article_AS400_vat") }}
    WHERE end_datetime IS null
),

result AS (
    SELECT
        sat_pa.* EXCEPT (promotion_article_hk),
        sat_pa_esv.* EXCEPT (promotion_article_hk),
        sat_pa_dmc.* EXCEPT (promotion_article_hk),
        sat_pa_dmv.* EXCEPT (promotion_article_hk),
        sat_pa_price.* EXCEPT (promotion_article_hk),
        sat_promotion_country.* EXCEPT (promotion_hk),
        sat_article_pp.* EXCEPT (article_hk, country),
        sat_article_ccp.* EXCEPT (article_hk, country),
        sat_article_vat.* EXCEPT (article_hk, country),
        hub_prom.promotion_bk AS promotion_code,
        hub_art.article_bk AS article_code,
        ref_dmc_description.discount_mechanism_description
    FROM lnk_promotion_article AS lnk
    LEFT JOIN hub_promotion AS hub_prom
        ON lnk.promotion_hk = hub_prom.promotion_hk
    LEFT JOIN hub_article AS hub_art
        ON lnk.article_hk = hub_art.article_hk
    LEFT JOIN sat_promotion_article AS sat_pa
        ON lnk.promotion_article_hk = sat_pa.promotion_article_hk
    LEFT JOIN sat_promotion_article_expected_sales_volume AS sat_pa_esv
        ON lnk.promotion_article_hk = sat_pa_esv.promotion_article_hk
    LEFT JOIN sat_promotion_article_discount_mechanism_code AS sat_pa_dmc
        ON lnk.promotion_article_hk = sat_pa_dmc.promotion_article_hk
    LEFT JOIN ref_promotion_article_discount_mechanism_description AS ref_dmc_description
        ON sat_pa_dmc.discount_mechanism_code = ref_dmc_description.discount_mechanism_code
    LEFT JOIN sat_promotion_article_discount_mechanism_value AS sat_pa_dmv
        ON lnk.promotion_article_hk = sat_pa_dmv.promotion_article_hk
    LEFT JOIN sat_promotion_article_price AS sat_pa_price
        ON lnk.promotion_article_hk = sat_pa_price.promotion_article_hk
    LEFT JOIN sat_promotion_country AS sat_promotion_country
        ON hub_prom.promotion_hk = sat_promotion_country.promotion_hk
    LEFT JOIN sat_article_purchase_price AS sat_article_pp
        ON
            hub_art.article_hk = sat_article_pp.article_hk
            AND sat_promotion_country.country = sat_article_pp.country
    LEFT JOIN sat_article_cash_and_carry_price AS sat_article_ccp
        ON
            hub_art.article_hk = sat_article_ccp.article_hk
            AND sat_promotion_country.country = sat_article_ccp.country
    LEFT JOIN sat_article_vat AS sat_article_vat
        ON
            hub_art.article_hk = sat_article_vat.article_hk
            AND sat_promotion_country.country = sat_article_vat.country
)

SELECT *
FROM result
