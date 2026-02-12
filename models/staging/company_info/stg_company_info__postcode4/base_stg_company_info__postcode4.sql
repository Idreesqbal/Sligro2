-- It will take huge processing to read through all files in the cloud storage.
-- Only latest arrived files are staged.
SELECT * FROM {{ source('company_info', 'Info_Postcode4') }}
WHERE
    true
{{ latest_dt('rv_sat__postcode_details', var('company_info_start_date')) }}
QUALIFY max(_file_name) OVER (PARTITION BY dt) = _file_name
