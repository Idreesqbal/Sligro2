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

WITH hierarchy AS (
    SELECT *
    FROM {{ ref('rv_sat__data_hub_customer_hierarchy') }}
    WHERE customerid IS NOT null AND registrationdate IS NOT null
),

{# Create new customer bk using business logic.
Customerids are recycled in the source system. #}
unique_key_creation AS (
    SELECT
        hierarchy.*,
        concat(hierarchy.customerid, '||', hierarchy.registrationdate) AS customer_bk,
        concat(hierarchy.parentcustomerid, '||', parent_customer.registrationdate) AS customer_parent_bk
    {# Joining with bv_bdv__customer_customer because we don't have
    the registration date of the parent from the hierachy table.#}
    FROM {{ ref('bv_bdv__customer_customer') }} AS parent_customer
    INNER JOIN hierarchy
        ON
            parent_customer.customerid = hierarchy.parentcustomerid
            AND cast(hierarchy.load_datetime AS date)
            BETWEEN parent_customer.registrationdate AND coalesce(
                cast(parent_customer.end_datetime AS date), '9999-12-31'
            )
    QUALIFY row_number() OVER (
        PARTITION BY
            parent_customer.customerid,
            parent_customer.registrationdate,
            hierarchy.parentcustomerid,
            hierarchy.customerid,
            hierarchy.registrationdate
        ORDER BY hierarchy.load_datetime DESC
    ) = 1
),

{# Hashing the BKs #}
create_hashed_columns AS (
    SELECT
        * EXCEPT (customer_hk, customer_parent_hk),
        upper(to_hex(sha256(customer_bk))) AS customer_hk,
        upper(to_hex(sha256(customer_parent_bk))) AS customer_parent_hk
    FROM unique_key_creation
),

{# End date the customer HK based on the start_datetime.
This is needed so that customers with the same ID but different registration
dates can be active at the same time and we don't loose history. #}
end_date_calculation AS (
    SELECT
        * EXCEPT (end_datetime),
        lag(load_datetime) OVER (
            PARTITION BY customer_hk, customer_parent_hk
            ORDER BY load_datetime DESC
        ) AS end_datetime
    FROM create_hashed_columns
)

SELECT *
FROM end_date_calculation
