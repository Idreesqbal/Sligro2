{#Around 150k customers were demoted and replaced with a new 7 digit code.
This table holds this information.#}

{{
  config(
    materialized='table',
    partition_by={
      "field": "LOAD_DATETIME",
      "data_type": "timestamp",
      "granularity": "month"
    }
  )
}}

WITH ref__customer_demoted AS (
    SELECT *
    FROM {{ ref('ref__customer_6_to_7_digits') }}
),

{# Create new customer bk using business logic.
Customerids are recycled in the source system. #}
unique_key_creation AS (
    SELECT
        concat(customer_bk, '||', orig_creatie_datum) AS customer_bk,
        concat(orig_klant_id, '||', orig_creatie_datum) AS demoted_customer_bk,
        start_datetime,
        end_datetime,
        load_datetime,
        record_source
    FROM ref__customer_demoted

),

{# Hashing the BKs #}
create_hashed_columns AS (
    SELECT
        *,
        upper(to_hex(sha256(customer_bk))) AS customer_hk,
        upper(to_hex(sha256(demoted_customer_bk))) AS demoted_customer_hk,
        upper(to_hex(sha256(concat(customer_bk, demoted_customer_bk)))) AS customer__demoted_customer_hk
    FROM unique_key_creation
),

{# End date the customer HK based on the start_datetime.
This is needed so that customers with the same ID but different registration
dates can be active at the same time and we don't loose history. #}
end_date_calculation AS (
    SELECT
        * EXCEPT (end_datetime),
        lag(start_datetime) OVER (
            PARTITION BY customer_hk
            ORDER BY start_datetime DESC
        ) AS end_datetime
    FROM create_hashed_columns
)

SELECT *
FROM end_date_calculation
