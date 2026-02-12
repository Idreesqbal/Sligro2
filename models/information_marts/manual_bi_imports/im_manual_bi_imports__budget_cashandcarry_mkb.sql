SELECT
    distribution_channel,
    business_unit,
    year,
    weeknumber,
    week_budget_cashandcarry,
    week_budget_cashandcarry_cumulative
FROM {{ source('gcs_manual_bi_target_imports', 'budget_cashandcarry_mkb') }}
