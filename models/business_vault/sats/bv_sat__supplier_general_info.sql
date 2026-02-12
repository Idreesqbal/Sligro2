{{ config(materialized='incremental') }}

WITH intercompany AS (
    SELECT DISTINCT supplier_hk
    FROM {{ ref('rv_sat__supplier_intercompany') }}
),

final AS (
    SELECT
        general.*,
        Cast(CASE WHEN intercompany.supplier_hk IS null THEN 0 ELSE 1 END AS boolean) AS intercompany_indicator
    FROM {{ ref('rv_sat__supplier_general_info') }} AS general
    LEFT JOIN intercompany ON general.supplier_hk = intercompany.supplier_hk
)

SELECT *
FROM final

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 AS exist
        FROM {{ this }} AS current_records
        WHERE
            final.supplier_hk = current_records.supplier_hk
    )
{% endif %}
