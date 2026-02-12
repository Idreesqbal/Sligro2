WITH
carrier_general_info AS (
    SELECT
        carrier_hk,
        lvnmc1 AS carrier_name,
        s017c1 AS status,
        NULLIF(TRIM(btnrc1), '') AS vat_number,
        NULLIF(TRIM(vlkdc1), '') AS payment_currency
    FROM {{ ref('bv_sat__carrier_general_info') }}
    WHERE end_datetime IS null
),

carrier_finance AS (
    SELECT
        carrier_hk,
        NULLIF(TRIM(ibanc1), '') AS bank_account_number,
        NULLIF(TRIM(swftc1), '') AS bic_swift_code
    FROM {{ ref('bv_sat__carrier_finance') }}
    WHERE end_datetime IS null
),

carrier_contacts AS (
    SELECT
        carrier_hk,
        NULLIF(TRIM(websc2), '') AS website
    FROM {{ ref('bv_sat__carrier_contacts') }}
    WHERE end_datetime IS null
),

-- source for chamber of commerce (kVK) can't be determined, SME and DDO prefer to keep it and filled with 'Unknown'
carrier AS (
    SELECT
        hub_carrier.carrier_bk AS pk_carrier_code,
        carrier_general_info.carrier_name,
        'Unknown' AS chamber_of_commerce_registration_number,
        carrier_general_info.vat_number,
        carrier_finance.bank_account_number,
        carrier_finance.bic_swift_code,
        carrier_general_info.payment_currency,
        carrier_contacts.website,
        carrier_general_info.status
    FROM {{ ref('rv_hub__carrier') }} AS hub_carrier
    INNER JOIN carrier_general_info
        ON hub_carrier.carrier_hk = carrier_general_info.carrier_hk
    LEFT JOIN carrier_finance
        ON hub_carrier.carrier_hk = carrier_finance.carrier_hk
    LEFT JOIN carrier_contacts
        ON hub_carrier.carrier_hk = carrier_contacts.carrier_hk
)

SELECT *
FROM carrier
