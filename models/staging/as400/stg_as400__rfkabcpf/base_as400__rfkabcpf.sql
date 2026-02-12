--This was givin as the possible descriptions for a (second but different) sales invoice type.

WITH program AS (
    SELECT DISTINCT
        wakdot AS program_code,
        trim(lgomot) AS program_desc
    FROM {{ source('sligro60_slgmutlib_slgfilelib', 'omstabpf') }}
    WHERE trim(omkdot) = 'RFKABCPF'
),

source AS (
    SELECT
        src_sales_invoice_delivery.*,
        program.program_desc
    FROM {{ source('sligro60_slgfilelib_slgfilelib', 'rfkabcpf') }} AS src_sales_invoice_delivery
    LEFT OUTER JOIN program ON trim(src_sales_invoice_delivery.pgmtc1) = trim(program.program_code)

)

SELECT *
FROM source
WHERE _fivetran_synced > '2024-12-16 10:00:00.000000'
