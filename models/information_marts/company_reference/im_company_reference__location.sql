WITH
{# sourced from Company Info Locaties file #}
sat_location AS (
    SELECT *
    FROM {{ ref("rv_masat__company_reference_location") }}
    WHERE end_datetime IS null
),

{# Enrich the sat_location CTE with the description of code for the listed columns below #}
location AS (
    {% set columns= ['SIZOSBIL', 'SIZOSBI2L', 'SIZOSBISEL',
                    'SIZKAM_LOC', 'SIZKLFTL', 'GW_KWHITEL',
                    'GW_KBLUEL', 'GW_KJVGASL', 'GW_KJVELEL'] %}
    SELECT
        sat_location.*,
        {{ ci_select_ref_labels(columns) }}
    FROM sat_location
    {{ ci_lookup_ref_labels(
        identifier='Locaties',
        columns=columns
    ) }}
),

{# sourced from Company Info Voorspelmodellen Locatie file #}
sat_location_prediction AS (
    SELECT *
    FROM {{ ref("rv_masat__company_reference_location_predictions") }}
    WHERE end_datetime IS null
),

{# Enrich the sat_location_prediction CTE with the description of code for the listed columns below #}
location_prediction AS (
    {% set columns= ['SIZCBGT'] %}
    SELECT
        sat_location_prediction.*,
        {{ ci_select_ref_labels(columns) }}
    FROM sat_location_prediction
    {{ ci_lookup_ref_labels(
        identifier='Voorspelmodellen_Locatie',
        columns=columns
    ) }}
),

result AS (
    SELECT
        location.sizolockey AS pk_company_reference_location_code,
        hub_company_reference.company_reference_bk AS fk_company_reference_code_ultimate_parent,
        hub_postcode.postcode_bk AS postal_code_4_digit,
        location.postcode6 AS postal_code_6_digit,
        location.sizosbil AS sbi_code_location,
        location.sizosbil_desc AS sbi_description,
        location.sizosbi2l AS sbi_code_2_digit_location,
        location.sizosbi2l_desc AS sbi_2_digit_description,
        location.sizosbisel AS sbi_code_segment_location,
        location.sizosbisel_desc AS sbi_code_segment_location_description,
        location.sizwam_loc AS number_of_employees_location,
        location.sizkam_loc_desc AS number_of_employees_class,
        location.sizwai_loc AS number_of_companies_location,
        location.sizwlftl AS company_age_location,
        location.sizklftl_desc AS age_class,
        location.sizwasbi2l AS number_of_unique_sbi_2_digit_location,
        location.gw_kwhitel_desc AS white_collar_class,
        location.gw_kbluel_desc AS blue_collar_class,
        location.gw_kjvgasl_desc AS annual_gas_consumption,
        location.gw_kjvelel_desc AS annual_electric_consumption,
        location_prediction.sizcbgt_desc AS location_prediction
    FROM {{ ref('rv_hub__company_reference') }} AS hub_company_reference
    INNER JOIN {{ ref('rv_lnk__company_reference_location') }} AS link_location
        ON hub_company_reference.company_reference_hk = link_location.company_reference_hk
    INNER JOIN {{ ref('rv_hub__postcode') }} AS hub_postcode
        ON link_location.postcode_hk = hub_postcode.postcode_hk
    INNER JOIN location
        ON link_location.company_reference_location_hk = location.company_reference_location_hk
    LEFT JOIN location_prediction -- not all location may have prediction model result
        ON
            location.company_reference_location_hk = location_prediction.company_reference_location_hk
            AND location.postcode6 = location_prediction.postcode6
)

SELECT *
FROM result
