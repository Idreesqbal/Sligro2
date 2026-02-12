-- It will take huge processing to read through all files in the cloud storage.
-- Only latest arrived files are staged.
SELECT * FROM {{ source('company_info', 'View_Webdata_Keywords_Concern') }}
WHERE
    true
{{ latest_dt('rv_sat__company_reference_webdata_keywords_ultimate_parent', var('company_info_start_date')) }}
QUALIFY max(_file_name) OVER (PARTITION BY dt) = _file_name
