{{ config(
    partition_by={
      "field": "LOAD_DATETIME",
      "data_type": "timestamp",
      "granularity": "month"
    }
  )
}}

{# Gathering the information about demoted customers. #}
WITH bv_bdv__customer_6_to_7_digits AS (
    SELECT * FROM {{ ref('bv_bdv__customer_6_to_7_digits') }}
    WHERE end_datetime IS null
),

{# Gathering the information about companies that are also a sligro customer. #}
rv_sat__company_reference_customer AS (
    SELECT
        * EXCEPT (end_datetime),
        "!bv_sat__company_reference_customer_matching" AS sat_customer_match
    FROM {{ ref('rv_sat__company_reference_customer_matching') }}
    WHERE klantkey IS NOT null AND startdatum_klant IS NOT null AND sizokey IS NOT null
),

{# Capture all matching of Sligro customer and company from tracking satellite#}
matching_key_presence AS (
    {{ ci_track_business_key_presence(
        rv_xts = ref('rv_xts__company_reference_matching_table'),
        pk = 'company_reference_customer_hk',
        tracked_sat_name = 'rv_sat__company_reference_customer_matching')
    }}
),

company_reference_customer AS (
    -- A customer can be re-matched multiple times by company info.
    -- The business need to see all occured re-matched (when the matching first and last appeared)
    SELECT
        rv_sat__company_reference_customer.*,
        matching_key_presence.first_appear,
        matching_key_presence.last_appear,
        matching_key_presence.is_in_latest_ci_file
    FROM {{ ref('rv_lnk__company_reference_customer_matching') }} AS link_customer_matching
    INNER JOIN
        matching_key_presence
        ON
            link_customer_matching.company_reference_customer_hk
            = matching_key_presence.company_reference_customer_hk
    INNER JOIN rv_sat__company_reference_customer
        ON
            link_customer_matching.company_reference_customer_hk
            = rv_sat__company_reference_customer.company_reference_customer_hk
),

{# Create new customer bk using business logic.
Customerids are recycled in the source system. #}
unique_key_creation AS (
    SELECT
        *,
        concat(trim(klantkey), "||", startdatum_klant) AS raw_customer_bk,
        upper(trim(sizokey)) AS company_reference_bk
    FROM company_reference_customer
),


{# Hashing the BKs #}
{#
    The original company_reference_customer_hk from the raw vault is the combination of company_reference bk + customer number
    However, sligro customer number is re-used.
    So to make it unique, in the BV the company_reference_customer_hk is replaced by the combination of company_reference bk + customer number + registration date
#}

create_hashed_columns AS (
    SELECT
        * EXCEPT (company_reference_customer_hk),
        upper(to_hex(sha256(raw_customer_bk))) AS raw_customer_hk,
        upper(to_hex(sha256(concat(company_reference_bk, "||", raw_customer_bk)))) AS raw_company_reference_customer_hk,
        upper(to_hex(sha256(company_reference_bk))) AS company_reference_hk
    FROM unique_key_creation
),

{# We need to adjust the raw_company_reference_customer_hk for when a customer is demoted.
In that case, we want to replace the demoted customer with the new one, and re-create the HK. #}
adjust_for_demoted_customers AS (
    SELECT
        create_hashed_columns.*,
        if(
            bv_bdv__customer_6_to_7_digits.demoted_customer_hk IS NOT null,
            bv_bdv__customer_6_to_7_digits.customer_hk,
            create_hashed_columns.raw_customer_hk
        ) AS customer_hk,
        if(
            bv_bdv__customer_6_to_7_digits.demoted_customer_bk IS NOT null,
            bv_bdv__customer_6_to_7_digits.customer_bk,
            create_hashed_columns.raw_customer_bk
        ) AS customer_bk,
        if(
            bv_bdv__customer_6_to_7_digits.demoted_customer_hk IS NOT null,
            true,
            false
        ) AS is_demoted_customer_indicator
    FROM create_hashed_columns
    LEFT JOIN
        bv_bdv__customer_6_to_7_digits
        ON create_hashed_columns.raw_customer_hk = bv_bdv__customer_6_to_7_digits.demoted_customer_hk
),

{# Re-creating the HK for demoted customers logic. #}
create_new_hk AS (
    SELECT
        *,
        upper(to_hex(sha256(concat(company_reference_bk, "||", customer_bk)))) AS company_reference_customer_hk
    FROM adjust_for_demoted_customers
),

{# End date the adjusted company_reference_customer_hk so that only one combination of customer and company reference HK
is valid at a given time.#}
end_date_calculation AS (
    SELECT
        *,
        lag(load_datetime) OVER (
            PARTITION BY company_reference_customer_hk
            ORDER BY load_datetime DESC
        ) AS end_datetime
    FROM create_new_hk
)

SELECT * FROM end_date_calculation
