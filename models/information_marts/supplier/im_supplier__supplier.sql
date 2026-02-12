WITH

sat_supplier_general AS (
    SELECT DISTINCT
        sat_supplier_general.supplier_hk,
        sat_supplier_general.ikbac2,
        sat_supplier_general.vlkdc1,
        sat_supplier_general.lvnmc1,
        sat_supplier_general.btnrc1,
        sat_supplier_general.btkdc1,
        sat_supplier_general.intercompany_indicator,
        sat_supplier_general.s017c1
    FROM {{ ref("bv_sat__supplier_general_info") }} AS sat_supplier_general
    WHERE sat_supplier_general.end_datetime IS null
),

lnk_supplier_carrier AS (
    SELECT
        rv_lnk__supplier__carrier.supplier_carrier_hk,
        rv_lnk__supplier__carrier.carrier_hk,
        hub_carrier.carrier_bk,
        rv_lnk__supplier__carrier.supplier_hk,
        rv_lnk__supplier__carrier.load_datetime
    FROM {{ ref("rv_lnk__supplier__carrier") }} AS rv_lnk__supplier__carrier
    INNER JOIN {{ ref('rv_hub__carrier') }} AS hub_carrier
        ON rv_lnk__supplier__carrier.carrier_hk = hub_carrier.carrier_hk
    QUALIFY
        ROW_NUMBER()
            OVER (
                PARTITION BY rv_lnk__supplier__carrier.supplier_hk ORDER BY rv_lnk__supplier__carrier.load_datetime DESC
            )
        = 1
),

sat_supplier_contacts AS (
    SELECT DISTINCT
        sat_supplier_contacts.supplier_hk,
        sat_supplier_contacts.websc2,
        sat_supplier_contacts.emalc2 AS order_receipt_mail,
        TRIM(REPLACE(sat_supplier_contacts.fxnrc4, 'NOOD', '')) AS emergency_number
    FROM {{ ref("rv_sat__supplier_contacts") }} AS sat_supplier_contacts
    WHERE
        sat_supplier_contacts.end_datetime IS null
        AND sat_supplier_contacts.websc2 != ''
),

sat_supplier_edi AS (
    SELECT DISTINCT
        sat_supplier_edi.supplier_hk,
        sat_supplier_edi.bstmc1
    FROM {{ ref("rv_sat__supplier_edi") }} AS sat_supplier_edi
    WHERE sat_supplier_edi.end_datetime IS null
),

sat_supplier_finance AS (
    SELECT
        sat_supplier_finance.supplier_hk,
        sat_supplier_finance.ibanc1,
        sat_supplier_finance.swftc1,
        sat_supplier_finance.nmplc1
    FROM {{ ref("rv_sat__supplier_finance") }} AS sat_supplier_finance
    WHERE sat_supplier_finance.end_datetime IS null
    GROUP BY
        sat_supplier_finance.supplier_hk,
        sat_supplier_finance.ibanc1,
        sat_supplier_finance.swftc1,
        sat_supplier_finance.nmplc1,
        sat_supplier_finance.update_ident
    QUALIFY
        DENSE_RANK()
            OVER (PARTITION BY sat_supplier_finance.supplier_hk ORDER BY sat_supplier_finance.update_ident DESC)
        = 1
)

SELECT
    hub_supplier.supplier_bk AS pk_supplier_code,
    NULLIF(CAST(sat_supplier_general.vlkdc1 AS STRING), '') AS fk_currency_code,
    lnk_supplier_carrier.carrier_bk AS fk_carrier_code,
    hub_supplier.supplier_bk AS supplier_number_as400,
    'Unknown' AS supplier_number_sap,
    sat_supplier_general.lvnmc1 AS supplier_name,
    NULLIF(CAST(sat_supplier_general.btkdc1 AS STRING), '') AS vat_code,
    NULLIF(CAST(sat_supplier_general.btnrc1 AS STRING), '') AS vat_number,
    sat_supplier_contacts.websc2 AS company_url,
    NULLIF(CAST(sat_supplier_contacts.emergency_number AS STRING), '') AS emergency_number,
    COALESCE(sat_supplier_general.intercompany_indicator, false) AS trade_relation_indicator,
    NULLIF(sat_supplier_contacts.order_receipt_mail, '') AS order_receipt_mail,
    sat_supplier_edi.bstmc1 AS gln_edi,
    NULLIF(CAST(sat_supplier_finance.ibanc1 AS STRING), '') AS bank_account_number,
    NULLIF(CAST(sat_supplier_finance.swftc1 AS STRING), '') AS bic_swift_code,
    NULLIF(sat_supplier_finance.nmplc1, '') AS bank,
    sat_supplier_general.s017c1 AS status
FROM {{ ref("rv_hub__supplier") }} AS hub_supplier
INNER JOIN sat_supplier_general
    ON hub_supplier.supplier_hk = sat_supplier_general.supplier_hk
LEFT JOIN sat_supplier_contacts
    ON hub_supplier.supplier_hk = sat_supplier_contacts.supplier_hk
LEFT JOIN sat_supplier_edi
    ON hub_supplier.supplier_hk = sat_supplier_edi.supplier_hk
LEFT JOIN lnk_supplier_carrier
    ON hub_supplier.supplier_hk = lnk_supplier_carrier.supplier_hk
LEFT JOIN sat_supplier_finance
    ON hub_supplier.supplier_hk = sat_supplier_finance.supplier_hk
