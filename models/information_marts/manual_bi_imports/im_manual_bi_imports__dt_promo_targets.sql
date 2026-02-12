SELECT
    brochure_description,
    brochure_target,
    business_unit,
    year
FROM {{ source('gcs_manual_bi_target_imports', 'dt_promo_targets') }}
