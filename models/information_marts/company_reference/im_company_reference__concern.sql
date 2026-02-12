WITH
{# sourced from Company Info Concerns file #}
sat_detail AS (
    SELECT *
    FROM {{ ref('rv_sat__company_reference_concern_details') }}
    WHERE end_datetime IS null
),

{# Enrich the sat_detail CTE with the description of code for the listed columns below #}
detail AS (
    {% set columns= [
    'SIZOSBIC', 'SIZOSBI2C', 'SIZOSBISEC', 'SIZKAM_CO',
    'SIZKLFTC', 'SIZCAFZGEB', 'SIZCMVOPYC', 'GW_KWHITEC', 'GW_KBLUEC', 'SELLLNKI'] %}
    SELECT
        sat_detail.*,
        {{ ci_select_ref_labels(columns) }}
    FROM sat_detail
    {{ ci_lookup_ref_labels(
        identifier='Concerns',
        columns=columns
    ) }}
),

{#
    Filter out deleted rows by only selecting data
    that are in the latest load cycle of Company Info View Webdata Keyword file
#}
active_webdata_keys AS (
    {{ ci_filter_deleted_rows(
        rv_hub_link = ref('rv_hub__company_reference'),
        rv_xts = ref('rv_xts__company_reference_view_webdata_keyword_concern'),
        pk = 'company_reference_hk',
        tracked_sat_name = 'rv_sat__company_reference_webdata_keywords_ultimate_parent'
        ) }}
),

{# Only select data rows that are in the latest load cycle of Company Info View Webdata Keyword file. #}
sat_webdata AS (
    SELECT sat.*
    FROM {{ ref('rv_sat__company_reference_webdata_keywords_ultimate_parent') }} AS sat
    INNER JOIN active_webdata_keys
        ON sat.company_reference_hk = active_webdata_keys.company_reference_hk
    WHERE sat.end_datetime IS null
),

{#
    Filter out deleted rows by only selecting data
    that are in the latest load cycle of Company Info Sizokeys_Opgeheven file
#}
latest_discontinued_company AS (
    {{ ci_filter_deleted_rows(
        rv_hub_link = ref('rv_hub__company_reference'),
        rv_xts = ref('rv_xts__company_reference_sizokeys_opgeheven'),
        pk = 'company_reference_hk',
        tracked_sat_name = 'rv_sat__company_reference_company_keys_deleted'
        ) }}
),

{# Only select data rows that are in the latest load cycle of Company Info Sizokeys_Opgeheven file. #}
sat_discontinued_company AS (
    SELECT sat.*
    FROM {{ ref('rv_sat__company_reference_company_keys_deleted') }} AS sat
    INNER JOIN latest_discontinued_company
        ON sat.company_reference_hk = latest_discontinued_company.company_reference_hk
    WHERE sat.end_datetime IS null
),

{# Enrich the sat_discontinued_company CTE with the description of code for the listed columns below #}
discontinued_company AS (
    {% set columns= ['SELLOPH','SELCOPH'] %}
    SELECT
        sat_discontinued_company.*,
        {{ ci_select_ref_labels(columns) }}
    FROM sat_discontinued_company
    {{ ci_lookup_ref_labels(
        identifier='SizoKeys_Opgeheven',
        columns=columns
    ) }}
),

{#
    The concern mart covers information of:
    - Details of the Concern (detail CTE)
    - Web-related information of the Concern (sat_webdata CTE)
    - Sligro customer category and segment based on Concern SBI code
    - Indicator whether the concern has been discontinued or not (discontinued_company CTE)
#}
result AS (
    SELECT
        hub_company.company_reference_bk AS pk_company_reference_code_ultimate_parent,
        detail.sizosbic AS sbi_code_concern,
        detail.sizosbic_desc AS sbi_concern_description,
        detail.sizosbi2c AS sbi_code_2_digit_concern,
        detail.sizosbi2c_desc AS sbi_code_2_digit_concern_description,
        detail.sizosbisec AS sbi_code_segment_concern,
        detail.sizosbisec_desc AS sbi_segment_concern_description,
        ref_sbi_mapping.category_code,
        ref_sbi_mapping.category_description,
        ref_sbi_mapping.segment_code,
        ref_sbi_mapping.segment_description,
        detail.sizwam_co AS number_of_employees_concern,
        detail.sizkam_co_desc AS number_of_employees_concern_class,
        detail.sizwai_co AS number_of_companies_concern,
        detail.sizwloc_co AS number_of_locations_concern,
        detail.sizwlftc AS company_age_concern,
        detail.sizklftc_desc AS company_age_class_concern,
        detail.sizwasbi2c AS number_of_unique_sbi_2_digit_concern,
        detail.sizcafzgeb_desc AS sales_area,
        detail.sizcmvopyc_desc AS csr_pyramid,
        detail.gw_kwhitec_desc AS white_collar_class_concern,
        detail.gw_kbluec_desc AS blue_collar_class_concern,
        detail.selllnki_desc AS is_suitable_for_linkedin_ads_indicator,
        (sat_webdata.wdilfacebo = 1) AS facebook_indicator,
        (sat_webdata.wdilinstgr = 1) AS instagram_indicator,
        (sat_webdata.wdillinkin = 1) AS linkedin_indicator,
        (sat_webdata.wdilwebsho = 1) AS is_webshop_indicator,
        (COALESCE(discontinued_company.selloph, 0) = 1) AS discontinuation_indicator,
        discontinued_company.selcoph_desc AS discontinuation_type,
        discontinued_company.siztophdat AS discontinuation_date
    FROM {{ ref('rv_hub__company_reference') }} AS hub_company
    INNER JOIN detail
        ON hub_company.company_reference_hk = detail.company_reference_hk
    LEFT JOIN sat_webdata
        ON detail.company_reference_hk = sat_webdata.company_reference_hk
    LEFT JOIN discontinued_company
        ON detail.company_reference_hk = discontinued_company.company_reference_hk
    LEFT JOIN {{ ref('ref__company_sbi_category_segment_mapping') }} AS ref_sbi_mapping
        ON detail.sizosbic = ref_sbi_mapping.sbi_code
)

SELECT *
FROM result
