{{
  config(
    partition_by={
      "field": "LOAD_DATETIME",
      "data_type": "timestamp",
      "granularity": "month"
    }
  )
}}

WITH header AS (
    SELECT
        load_datetime,
        start_datetime,
        end_datetime,
        ionrba,
        purchase_order_bk,
        purchase_order_hk
    FROM {{ ref('bv_bdv__purchase_order') }}
),

header_extended AS (
    SELECT
        finac1,
        fokdc1,
        ictpc1,
        ionrc1,
        ornrc4,
        ornrc5,
        safrc1,
        update_ident,
        load_datetime,
        start_datetime,
        end_datetime
    FROM {{ ref('rv_sat__purchase_order_extended') }}
),

{# Add new purchase order bk and hk using business logic from the header.
We need to join with the header because we don't have the received date in the header extended.
We use QUALIFY to remove duplicates. #}
unique_key_creation AS (
    SELECT
        header_extended.finac1,
        header_extended.fokdc1,
        header_extended.ictpc1,
        header_extended.ionrc1,
        header_extended.ornrc4,
        header_extended.ornrc5,
        header_extended.safrc1,
        header_extended.update_ident,
        header_extended.load_datetime,
        header_extended.start_datetime,
        header_extended.end_datetime,
        header.purchase_order_bk,
        header.purchase_order_hk
    FROM header_extended
    INNER JOIN header
        ON
            header_extended.ionrc1 = header.ionrba
            AND cast(header_extended.start_datetime AS date) BETWEEN
            cast(header.start_datetime AS date) AND coalesce(cast(header.end_datetime AS date), "9999-12-31")
    QUALIFY row_number() OVER (
        PARTITION BY header.purchase_order_bk, header_extended.start_datetime, header_extended.fokdc1
        ORDER BY header_extended.start_datetime ASC, header_extended.load_datetime DESC
    ) = 1
),

{# End date the purchase_order_hk based on the start_datetime.
This is needed so that orders with the same ID but different creation
dates can be active at the same time and we don't loose history. #}
end_date_calculation AS (
    SELECT
        * EXCEPT (end_datetime),
        lag(start_datetime) OVER (
            PARTITION BY purchase_order_hk
            ORDER BY start_datetime DESC
        ) AS end_datetime
    FROM unique_key_creation
)

SELECT * FROM end_date_calculation
