-- No partition is needed for this satellite because the number of rows is relatively small (< 100)
{{ config(materialized='incremental') }}

WITH
record_to_insert AS (
    -- Filter out rv_sat__supplier_contacts for supplier that acts as carrier and preserved the historical data.
    SELECT
        link_carrier_supplier.carrier_hk,
        sat_supplier_contacts.*
    FROM {{ ref("rv_hub__supplier") }} AS hub_supplier
    INNER JOIN
        {{ ref("rv_sat__supplier_contacts") }} AS sat_supplier_contacts
        ON hub_supplier.supplier_hk = sat_supplier_contacts.supplier_hk
    INNER JOIN
        {{ ref('rv_lnk__supplier__carrier') }} AS link_carrier_supplier
        ON hub_supplier.supplier_hk = link_carrier_supplier.supplier_hk
)

-- This business vault satellite is sourced from a supplier's satellite.
-- All attributes from rv_sat__supplier_contacts are needed, except supplier_hk
-- The record source is replaced into rv_sat__supplier_contacts instead of AS400 table
-- Load data where records do not exist or if end_datetime is updated
SELECT * EXCEPT (supplier_hk) REPLACE ('rv_sat__supplier_contacts' AS record_source) FROM record_to_insert
{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 AS exist
        FROM {{ this }} AS current_records
        WHERE
            record_to_insert.carrier_hk = current_records.carrier_hk
            AND record_to_insert.load_datetime = current_records.load_datetime
            AND record_to_insert.end_datetime IS NOT DISTINCT FROM current_records.end_datetime
    )
{% endif %}
