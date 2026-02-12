-- It will take huge processing to read through all files in the cloud storage.
-- Only latest arrived files are staged.
-- Handles incorrect date format (example: 09/06/2023 00:00:00 instead of 2023-06-09)
SELECT
    * REPLACE (
        COALESCE(
            SAFE.PARSE_DATE('%Y-%m-%d', startdatum_klant),
            DATE(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', startdatum_klant))
        ) AS startdatum_klant
    )
FROM {{ source('company_info', 'MatchingTable') }}
WHERE
    true
    AND SAFE.PARSE_DATE('%Y-%m-%d', startdatum_klant) IS NOT null
{{ latest_dt('rv_sat__company_reference_customer_matching', var('company_info_start_date')) }}
QUALIFY MAX(_file_name) OVER (PARTITION BY dt) = _file_name
