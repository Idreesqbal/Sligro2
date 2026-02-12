-- Select only the rows with rank 1 resulting in a table where only the last scan of each date is selected.
-- There should be only 1 version of a scan on each day.
SELECT * FROM {{ ref('stg_soda_check_results')}}
WHERE
    scan_rank = 1
    AND scan_date >= '2024-10-01'
    AND check_name NOT LIKE '%test%'
    AND (RIGHT(check_name, 2) = '_C' OR RIGHT(check_name, 2) = '_V')
