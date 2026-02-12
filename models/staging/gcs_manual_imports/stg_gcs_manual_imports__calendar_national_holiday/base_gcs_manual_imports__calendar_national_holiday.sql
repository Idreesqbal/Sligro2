-- The national holiday data is manually ingested through csv files.
-- normalize data rows before entering raw vault
SELECT
    calendar_date_nk,
    holiday_name,
    holiday_type,
    holiday_start_date,
    holiday_end_date,
    country_region,
    country
FROM
    {{ source('gcs_manual_imports', 'calendar_national_holiday') }},
    UNNEST(GENERATE_DATE_ARRAY(holiday_start_date, holiday_end_date)) AS calendar_date_nk
