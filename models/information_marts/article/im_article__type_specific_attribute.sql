WITH hub_article AS (
    SELECT
        article_hk,
        article_bk
    FROM
        {{ ref("rv_hub__article") }}
),

sat_article_type_specific_attribute AS (
    SELECT *
    FROM
        {{ ref("bv_masat__article_type_specific_attribute") }}
    WHERE
        end_datetime IS null
        AND is_deleted = false
),

result AS (
    SELECT
        * EXCEPT (article_bk, locale), -- noqa: RF02
        hub.article_bk AS fk_article_code,
        sat.locale AS fk_locale_code,
        CONCAT(
            hub.article_bk, "||",
            sat.locale
        ) AS pk_article_type_specific_attribute_code
    FROM
        hub_article AS hub
    INNER JOIN sat_article_type_specific_attribute AS sat
        ON hub.article_hk = sat.article_hk
)

-- noqa: disable=all
SELECT
    pk_article_type_specific_attribute_code,
    fk_article_code,
    fk_locale_code,
    * EXCEPT (
            article_hk,
            pk_article_type_specific_attribute_code,
            fk_article_code,
            fk_locale_code,
            is_deleted,
            is_created,
            hashdiff,
            record_source,
            load_datetime,
            end_datetime)
FROM
    result
