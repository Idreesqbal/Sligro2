--This was givin as the possible descriptions for a sales invoice type.
WITH sales_invoice_type AS (
    SELECT DISTINCT
        ictpc1 AS invoice_type_code,
        trim(icomc1) AS invoice_type_desc
    FROM {{ source('sligro60_slgmutlib_slgfilelib', 'ictpscpf') }}
),

source AS (
    SELECT
        src_sales_invoice.*,
        'Eur' AS currency_code, -- Ruben van de Donk: Agreed with Koen for this addition
        sales_invoice_type.invoice_type_code,
        sales_invoice_type.invoice_type_desc
    FROM {{ source('sligro60_slgfilelib_slgfilelib', 'dwfktcpf') }} AS src_sales_invoice
    LEFT JOIN sales_invoice_type ON src_sales_invoice.fktpc1 = sales_invoice_type.invoice_type_code

)

SELECT *
FROM source
WHERE _fivetran_synced > '2024-12-16 10:00:00.000000'
