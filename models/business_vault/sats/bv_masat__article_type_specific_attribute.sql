{{
  config(
    materialized='view'
  )
}}
-- noqa: disable=all
{% set data_query -%}
SELECT
    *
FROM
    {{ ref("rv_masat__article_type_specific_attribute") }}
{%- endset -%}

{#- The raw vault satellite IS structured IN LONG format, i.e.2 COLUMNS containg ATTRIBUTE - VALUE pairs.
For interpretation purposes we pivot this TO A wide TABLE containing A column for each ATTRIBUTE.-#}

WITH raw_attributes AS (
    {{ data_query }}
),

distinct_columns AS (
    SELECT DISTINCT
        name
    FROM
        raw_attributes
)


SELECT
    ARTICLE_HK,
    locale,
    hashdiff,
    raw_attributes.LOAD_DATETIME,
    LAG(raw_attributes.LOAD_DATETIME)
        OVER (
            PARTITION BY ARTICLE_HK, locale
            ORDER BY raw_attributes.LOAD_DATETIME DESC -- noqa
        ) AS END_DATETIME,
    {%- for column in get_distinct_column_values_from_query(data_query, 'name') %}
    MAX(CASE WHEN name = '{{ column }}' THEN value END) AS {{ sfg_convert_to_snake_case(column) }},
    {%- endfor %}
    is_deleted,
    is_created,
    record_source
FROM
    raw_attributes
GROUP BY
    ARTICLE_HK,
    locale,
    hashdiff,
    LOAD_DATETIME,
    is_deleted,
    is_created,
    record_source
