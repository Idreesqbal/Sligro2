-- It will take huge processing to read through all files in the cloud storage.
-- Only latest arrived files are staged.
SELECT * FROM {{ source('company_info', 'Concerns') }}
WHERE
    true
{{ latest_dt('rv_sat__company_reference_concern_details', var('company_info_start_date')) }}
QUALIFY max(_file_name) OVER (PARTITION BY dt) = _file_name
