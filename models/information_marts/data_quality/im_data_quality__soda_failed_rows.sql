{{
  config(
    materialized = 'incremental',
    unique_key = 'pk_failed_rows_code',
    partition_by={
        "field": "check_datetime",
        "data_type": "datetime",
        "granularity": "day"
        }
    )
}}

-- FAILED ROWS
-- This Query will result in a table with an overwiew of every failed row from Soda.
-----------------------------------------------------------------------------------------
WITH failed_rows AS (
    SELECT
        failed_rows_v2.generated_export_id AS failed_rows_generated_export_id,
        DATETIME(TIMESTAMP_SECONDS(CAST(CAST(failed_rows_v2.date AS FLOAT64) / 1e9 AS INT64)), "Europe/Amsterdam")
            AS failed_rows_check_datetime,
        failed_rows_v2.soda_check_name AS failed_rows_check_name,
        failed_rows_v2.soda_dataset AS failed_rows_dataset_name,
        (SELECT value FROM UNNEST(json_row) WHERE key = "check_value") AS failed_rows_check_value,
        NULLIF(
            (
                SELECT value
                FROM UNNEST(json_row)
                WHERE
                    key = "article_code"
                    OR key = "article_code1"
                    OR key = "article_article_code"
                    OR key = "article_article_code1"
            ),
            "None"
        ) AS failed_rows_article_code,
        NULLIF(
            (
                SELECT value
                FROM UNNEST(json_row)
                WHERE key = "gtin" OR key = "gtin1" OR key = "trade_item_gtin" OR key = "trade_item_gtin1"
            ),
            "None"
        ) AS failed_rows_trade_item_code,
        NULLIF(
            (SELECT value FROM UNNEST(json_row) WHERE key = "locale" OR key = "locale1"),
            "None"
        ) AS failed_rows_locale_code
    FROM {{ source('sfg_soda', 'failed_rows_v2') }} AS failed_rows_v2
    WHERE
        CAST(
            FORMAT_TIMESTAMP(
                "%Y-%m-%d",
                TIMESTAMP_SECONDS(CAST(CAST(failed_rows_v2.date AS FLOAT64) / 1e9 AS INT64)),
                "Europe/Amsterdam"
            ) AS DATE
        )
        >= "2025-01-01"
        AND soda_scan_definition NOT LIKE "%testScanDefinition%"
        AND soda_check_name NOT LIKE "%test%"
        AND LEFT(soda_check_name, 2) != "DH"
        AND (RIGHT(soda_check_name, 2) = "_C" OR RIGHT(soda_check_name, 2) = "_V")
),

-- Add a rank so when there are multiple checks on 1 date, the most recent check gets rank 1.
-- This is later used to filter on the most recent
failed_rows_with_rank AS (
    SELECT
        failed_rows_generated_export_id,
        failed_rows_check_name,
        failed_rows_dataset_name,
        failed_rows_check_datetime,
        failed_rows_check_value,
        failed_rows_article_code,
        failed_rows_trade_item_code,
        failed_rows_locale_code,
        CONCAT(failed_rows_article_code, "||", failed_rows_locale_code) AS failed_rows_article_locale_code,
        CONCAT(failed_rows_trade_item_code, "||", failed_rows_locale_code) AS failed_rows_trade_item_locale_code,
        DENSE_RANK()
            OVER (
                PARTITION BY failed_rows_check_name, DATE(failed_rows_check_datetime)
                ORDER BY failed_rows_check_datetime DESC
            )
            AS failed_rows_rank
    FROM failed_rows
    GROUP BY ALL
    QUALIFY DENSE_RANK()
        OVER (
            PARTITION BY failed_rows_check_name, DATE(failed_rows_check_datetime)
            ORDER BY failed_rows_check_datetime DESC
        )
    = 1
),

-- failed_rows_rank_1 AS (
--     SELECT * FROM failed_rows_with_rank
--     WHERE failed_rows_rank = 1
--     GROUP BY ALL
-- ),
-----------------------------------------------------------------------------------------

-- CHECK_RESULTS
-- This query is a historical list of al checks that were completed.
-- The check results are filtered almost the same way as the failed rows.
-----------------------------------------------------------------------------------------
check_results AS (
    SELECT
        check_results.result_id,
        check_results.check_id,
        check_results.check_name,
        soda_datasets.dataset_name,
        check_results.metric_value AS check_metric_value,
        DATETIME(TIMESTAMP(check_results.scan_time), "Europe/Amsterdam") AS scan_datetime,
        CASE
            WHEN
                LEAD(DATETIME(TIMESTAMP(check_results.scan_time), "Europe/Amsterdam")) OVER (
                    PARTITION BY check_results.check_name
                    ORDER BY DATETIME(TIMESTAMP(check_results.scan_time), "Europe/Amsterdam")
                ) IS null
                THEN DATETIME_ADD(DATETIME(TIMESTAMP(check_results.scan_time), "Europe/Amsterdam"), INTERVAL 4 HOUR)
            ELSE
                LEAD(DATETIME(TIMESTAMP(check_results.scan_time), "Europe/Amsterdam")) OVER (
                    PARTITION BY check_results.check_name
                    ORDER BY DATETIME(TIMESTAMP(check_results.scan_time), "Europe/Amsterdam")
                )
        END AS next_scan_datetime
    FROM {{ source('sfg_soda', 'check_results_v2') }} AS check_results
    LEFT JOIN {{ source('sfg_soda', 'datasets') }} AS soda_datasets
        ON check_results.dataset_id = soda_datasets.dataset_id
    WHERE
        DATE(DATETIME(TIMESTAMP(check_results.scan_time), "Europe/Amsterdam")) >= "2025-01-01"
        AND NULLIF((SELECT value FROM UNNEST(check_results.attributes) WHERE key = "check_status"), "None") = "Live"
        AND check_results.check_name NOT LIKE "%test%"
        AND (RIGHT(check_results.check_name, 2) = "_C" OR RIGHT(check_results.check_name, 2) = "_V")
),

-----------------------------------------------------------------------------------------

-- UNION TABLE
-- This query combines the failed rows query and the check_results query based on the fields:
-- failed_rows_check_name, failed_rows_dataset_name and failed_rows_check_datetime.
-- A union is used to add all the check results that have 0 failed rows.
-----------------------------------------------------------------------------------------
union_table AS (
    SELECT
        fr.failed_rows_generated_export_id AS fk_generated_export_code,
        fr.failed_rows_article_code AS fk_article_code,
        fr.failed_rows_trade_item_code AS fk_trade_item_code,
        fr.failed_rows_locale_code AS fk_locale_code,
        fr.failed_rows_article_locale_code AS fk_article_locale_code,
        fr.failed_rows_trade_item_locale_code AS fk_trade_item_locale_code,
        fr.failed_rows_check_datetime AS check_datetime,
        fr.failed_rows_check_value AS check_value,
        cr.scan_datetime,
        cr.result_id AS fk_check_result_code,
        fr.failed_rows_check_name,
        fr.failed_rows_dataset_name,
        cr.check_name,
        cr.dataset_name,
        cr.check_metric_value,
        1 AS num_failed_rows,
        true AS failed_row_indicator
    FROM failed_rows_with_rank AS fr
    INNER JOIN check_results AS cr
        ON
            fr.failed_rows_check_name = cr.check_name
            AND fr.failed_rows_check_datetime >= cr.scan_datetime
            AND fr.failed_rows_check_datetime < cr.next_scan_datetime
            AND cr.check_metric_value != 0

    UNION ALL

    SELECT
        null AS fk_generated_export_code,
        null AS fk_article_code,
        null AS fk_trade_item_code,
        null AS fk_locale_code,
        null AS fk_article_locale_code,
        null AS fk_trade_item_locale_code,
        null AS check_datetime,
        null AS check_value,
        scan_datetime,
        result_id AS fk_check_result_code,
        null AS failed_rows_check_name,
        null AS failed_rows_dataset_name,
        check_name,
        dataset_name,
        CAST(check_metric_value AS INT64) AS check_metric_value,
        0 AS num_failed_rows,
        false AS failed_row_indicator
    FROM check_results
    WHERE check_metric_value = 0 -- Only UNION all check results with 0 failed rows.
),
-----------------------------------------------------------------------------------------

-- COMBINED TABLE
-- In the combined table the duplicate columns are combined in to single columns
-----------------------------------------------------------------------------------------
combined_table AS (
    SELECT
        COALESCE(fk_generated_export_code, fk_check_result_code)
            AS fk_generated_export_code,
        CASE
            WHEN fk_trade_item_locale_code IS NOT null THEN fk_trade_item_locale_code
            WHEN fk_article_locale_code IS NOT null THEN fk_article_locale_code
            WHEN fk_trade_item_code IS NOT null THEN fk_trade_item_code
            ELSE fk_article_code
        END AS fk_entity_code,
        fk_article_code,
        fk_trade_item_code,
        fk_locale_code,
        fk_article_locale_code,
        fk_trade_item_locale_code,
        CASE WHEN fk_check_result_code IS null THEN check_datetime ELSE scan_datetime END AS scan_datetime,
        COALESCE(check_datetime, scan_datetime) AS check_datetime,
        CASE WHEN fk_check_result_code IS null THEN failed_rows_dataset_name ELSE dataset_name END AS dataset_name,
        CASE WHEN fk_check_result_code IS null THEN failed_rows_check_name ELSE check_name END AS check_name,
        check_value,
        fk_check_result_code,
        CAST(check_metric_value AS INT64) AS check_metric_value,
        num_failed_rows,
        failed_row_indicator
    FROM union_table
),
-----------------------------------------------------------------------------------------

-- COMBINED TABLE WITH RANK
-- In the combined table with rank, a rank is added that is the same as the rank that was used in failed rows.
-- This rank is used to add the same logic. It is used to filter out the check_results with 0 fails
-- that are not the latest checks on a specific date.
-----------------------------------------------------------------------------------------
combined_table_with_rank AS (
    SELECT
        CONCAT(
            COALESCE(fk_generated_export_code, ""), "||",
            COALESCE(fk_entity_code, ""), "||",
            COALESCE(check_name, ""), "||",
            COALESCE(dataset_name, ""), "||",
            COALESCE(CAST(check_datetime AS STRING), ""), "||",
            COALESCE(check_value, "")
        ) AS pk_failed_rows_code,
        fk_generated_export_code,
        fk_entity_code,
        fk_article_code,
        fk_trade_item_code,
        fk_article_locale_code,
        fk_trade_item_locale_code,
        scan_datetime,
        check_datetime,
        dataset_name,
        check_name,
        fk_check_result_code,
        check_value,
        num_failed_rows,
        failed_row_indicator,
        DENSE_RANK()
            OVER (PARTITION BY check_name, DATE(check_datetime) ORDER BY check_datetime DESC)
            AS failed_rows_rank
    FROM combined_table
    WHERE
        fk_check_result_code IS NOT null
    QUALIFY DENSE_RANK()
        OVER (PARTITION BY check_name, DATE(check_datetime) ORDER BY check_datetime DESC)
    = 1
)
-----------------------------------------------------------------------------------------

-- END RESULT
-----------------------------------------------------------------------------------------
SELECT *
FROM combined_table_with_rank
