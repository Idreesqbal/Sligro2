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

WITH rv_sat__customer_groups AS (
    SELECT *
    FROM {{ ref('rv_sat__data_hub_customer_groups') }}
    WHERE customerid IS NOT null AND registrationdate IS NOT null
),

{# Create new customer bk using business logic.
Customerids are recycled in the source system. #}
unique_key_creation AS (
    SELECT
        *,
        concat(customerid, '||', registrationdate) AS customer_bk,
        concat(customerid, '||', registrationdate, '||', groupid) AS customer_group_bk
    FROM rv_sat__customer_groups
),

{# Hashing the BKs #}
create_hashed_columns AS (
    SELECT
        * EXCEPT (customer_hk, customer_group_hk),
        upper(to_hex(sha256(customer_bk))) AS customer_hk,
        upper(to_hex(sha256(customer_group_bk))) AS customer_group_hk
    FROM unique_key_creation
),

{# End date the customer HK based on the start_datetime.
This is needed so that customers with the same ID but different registration
dates can be active at the same time and we don't loose history. #}
end_date_calculation AS (
    SELECT
        * EXCEPT (end_datetime),
        lag(load_datetime) OVER (
            PARTITION BY customer_hk, customer_group_hk
            ORDER BY load_datetime DESC
        ) AS end_datetime
    FROM create_hashed_columns
)

SELECT *
FROM end_date_calculation
