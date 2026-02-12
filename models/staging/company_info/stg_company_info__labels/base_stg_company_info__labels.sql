-- Only latest arrived file is staged.
SELECT * FROM {{ source('company_info', 'Labels') }}
WHERE dt = (SELECT MAX(dt) AS latest_dt FROM {{ source('company_info', 'Labels') }})
