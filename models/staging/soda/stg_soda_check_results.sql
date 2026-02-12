{{
  config(
    materialized = 'table'
    )
}}

WITH base AS (
    SELECT
        cr_v2.result_id AS pk_check_result_code,
        cr_v2.check_id AS fk_check_code,
        cr_v2.owner_id AS fk_owner_code,
        cr_v2.dataset_id AS fk_dataset_code,
        cr_v2.check_name,
        cr_v2.level,
        cr_v2.owner_email,
        soda_datasets.dataset_name,
        DATETIME(TIMESTAMP(cr_v2.scan_time), "Europe/Amsterdam") AS scan_datetime,
        DENSE_RANK()
            OVER (
                PARTITION BY cr_v2.check_name, DATE(DATETIME(TIMESTAMP(cr_v2.scan_time), "Europe/Amsterdam"))
                ORDER BY cr_v2.scan_time DESC
            )
            AS scan_rank,
        CAST(cr_v2.metric_value AS INT64) AS check_metric_value,
        DATE(DATETIME(TIMESTAMP(cr_v2.scan_time), "Europe/Amsterdam")) AS scan_date,
        CASE
            WHEN
                LEAD(DATETIME(TIMESTAMP(cr_v2.scan_time), "Europe/Amsterdam")) OVER (
                    PARTITION BY cr_v2.check_name ORDER BY DATETIME(TIMESTAMP(cr_v2.scan_time), "Europe/Amsterdam")
                ) IS null
                THEN DATETIME_ADD(DATETIME(TIMESTAMP(cr_v2.scan_time), "Europe/Amsterdam"), INTERVAL 4 HOUR)
            ELSE
                LEAD(DATETIME(TIMESTAMP(cr_v2.scan_time), "Europe/Amsterdam")) OVER (
                    PARTITION BY cr_v2.check_name ORDER BY DATETIME(TIMESTAMP(cr_v2.scan_time), "Europe/Amsterdam")
                )
        END AS next_scan_datetime,
        DATETIME(TIMESTAMP(cr_v2.created_at), "Europe/Amsterdam") AS check_created_datetime,
        NULLIF((SELECT value FROM UNNEST(cr_v2.attributes) WHERE key = "dimension"), "None") AS check_dimension,
        NULLIF((SELECT value FROM UNNEST(cr_v2.attributes) WHERE key = "definition"), "None") AS check_definition,
        NULLIF((SELECT value FROM UNNEST(cr_v2.attributes) WHERE key = "check_status"), "None") AS check_status,
        NULLIF((SELECT value FROM UNNEST(cr_v2.attributes) WHERE key = "version"), "None") AS check_version,
        NULLIF((SELECT value FROM UNNEST(cr_v2.attributes) WHERE key = "group"), "None") AS check_group,
        NULLIF((SELECT value FROM UNNEST(cr_v2.attributes) WHERE key = "definition_technical"), "None")
            AS check_definition_technical
    FROM {{ source('sfg_soda', 'check_results_v2') }} AS cr_v2
    LEFT JOIN {{ source('sfg_soda', 'datasets') }} AS soda_datasets ON cr_v2.dataset_id = soda_datasets.dataset_id
    WHERE
        cr_v2.check_name NOT LIKE "%test%"
        {% if is_incremental() %}
            AND DATETIME(TIMESTAMP(cr_v2.scan_time), "Europe/Amsterdam")
            >= COALESCE((SELECT MAX(scan_datetime) AS max_ldts FROM {{ this }}), "1900-01-01")
        {% else %}
            AND DATETIME(TIMESTAMP(cr_v2.scan_time), "Europe/Amsterdam") >= '2024-10-01'
  {% endif %}

)

SELECT * FROM base
