SELECT
    distribution_channel,
    business_unit,
    year,
    monthnumber,
    month_budget_cashandcarry,
    month_budget_cashandcarry_cumulative
FROM {{ source('gcs_manual_bi_target_imports', 'budget_month_cashandcarry') }}
