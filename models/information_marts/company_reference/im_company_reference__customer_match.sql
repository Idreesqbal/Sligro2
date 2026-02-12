WITH
sat_customer_matching AS (
    SELECT *
    FROM {{ ref('bv_bdv__company_reference_customer_matching') }}
    WHERE end_datetime IS null
),

result AS (
    SELECT
        CONCAT(
            hub_company.company_reference_bk,
            '||',
            hub_customer.customer_bk
        ) AS pk_company_reference_customer_match_code,
        hub_company.company_reference_bk AS fk_company_reference_code,
        -- to align with composition of customer code primary key with the data_hub customer
        -- (customer code + registration date)
        hub_customer.customer_bk AS fk_customer_code,
        sat_customer_matching.startdatum_klant AS customer_registration_date,
        (sat_customer_matching.opgeheven_jn = 1) AS discontinuation_indicator,
        sat_customer_matching.datum_opgeheven AS discontinuation_date,
        sat_customer_matching.naam_score AS name_based_score,
        sat_customer_matching.adres_score AS address_based_score,
        sat_customer_matching.kvk_score AS coc_based_score,
        sat_customer_matching.telefoon_score AS phone_based_score,
        sat_customer_matching.opgehevend_score AS canceled_score,
        sat_customer_matching.totaal_score AS total_score,
        DATE(sat_customer_matching.first_appear) AS first_matching_date,
        DATE(sat_customer_matching.last_appear) AS last_matching_date,
        sat_customer_matching.is_in_latest_ci_file AS recent_match_indicator
    FROM {{ ref('bv_lnk__company_reference_customer_matching') }} AS link_customer_matching
    INNER JOIN sat_customer_matching
        ON
            link_customer_matching.company_reference_customer_hk
            = sat_customer_matching.company_reference_customer_hk
    INNER JOIN {{ ref('rv_hub__company_reference') }} AS hub_company
        ON link_customer_matching.company_reference_hk = hub_company.company_reference_hk
    INNER JOIN {{ ref('bv_hub__customer') }} AS hub_customer
        ON link_customer_matching.customer_hk = hub_customer.customer_hk
    WHERE sat_customer_matching.is_demoted_customer_indicator = false
)

SELECT *
FROM result
