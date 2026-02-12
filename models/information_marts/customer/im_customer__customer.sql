WITH

hub_customer AS (
    SELECT
        customer_hk,
        customer_bk
    FROM {{ ref("bv_hub__customer") }}
),

sal_customer_demoted AS (
    SELECT
        customer_hk,
        demoted_customer_hk
    FROM {{ ref('bv_salnk__customer__demoted_customer') }}
),

bdv_customer_customer AS (
    SELECT
        customer_hk,
        customerid,
        registrationdate,
        tradename,
        subsegmentdescription,
        subsegmentcode,
        statusreasondescription,
        segmentdescription,
        segmentcode,
        sbicode,
        sbidescription,
        SPLIT(sbi2code_string, ',') AS sbi2code,
        SPLIT(sbi2description_string, ',') AS sbi2description,
        formulatype,
        customerof,
        headofficeindicator,
        origin,
        statuscode,
        nationalaccountlevel,
        nationalaccountsegment,
        nationalaccountdescription,
        statusdescription,
        owner,
        statutoryname,
        legalform,
        chamberofcommercenumber,
        chamberofcommerceestablishmentnumber,
        countrycode,
        category,
        accountmanageremployeeid,
        is_deleted,
        load_datetime,
        end_datetime
    FROM {{ ref('bv_bdv__customer_customer') }}
    WHERE end_datetime IS null
),

final AS (
    SELECT
        bdv_customer_customer.*,
        hub_customer.customer_bk
    FROM hub_customer
    INNER JOIN bdv_customer_customer
        ON hub_customer.customer_hk = bdv_customer_customer.customer_hk
    LEFT JOIN sal_customer_demoted
        ON hub_customer.customer_hk = sal_customer_demoted.demoted_customer_hk
    {# Removing demoted customers, otherwise we have duplicates in our data. #}
    WHERE sal_customer_demoted.demoted_customer_hk IS null
)

SELECT
    customer_bk AS pk_customer_code,
    customerid AS customer_code,
    registrationdate AS registration_date,
    tradename AS trade_name,
    subsegmentdescription AS segment_description,
    subsegmentcode AS segment_code,
    statusreasondescription AS deregistration_reason,
    sbicode AS sbi_code,
    sbidescription AS sbi_description,
    sbi2code AS sbi2_code,
    sbi2description AS sbi2_description,
    formulatype AS formula_type,
    customerof AS customer_of,
    headofficeindicator AS head_office_indicator,
    origin,
    statuscode AS status_code,
    nationalaccountlevel AS national_account_level,
    statusdescription AS status_description,
    statutoryname AS statutory_name,
    legalform AS legal_form,
    chamberofcommercenumber AS chamber_of_commerce_number,
    chamberofcommerceestablishmentnumber AS chamber_of_commerce_establishment_number,
    category AS category_code,
    accountmanageremployeeid AS account_manager_employee_id
FROM final
