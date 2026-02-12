-- It will take huge processing to read through all files in the cloud storage.
-- Only latest arrived files are staged.
-- Handles incorrect date format (example: 10/30/1992 00:00:00 instead of 30-10-1992)
SELECT
    * REPLACE (
        COALESCE(
            SAFE.PARSE_DATE('%d-%m-%Y', geboortedatum), DATE(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', geboortedatum))
        ) AS geboortedatum
    )
FROM {{ source('company_info', 'Contactpersonen') }}
WHERE
    true
{{ latest_dt('rv_masat__company_reference_contact_person', var('company_info_start_date')) }}
QUALIFY MAX(_file_name) OVER (PARTITION BY dt) = _file_name
