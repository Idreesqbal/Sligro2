WITH
{# sourced from Company Info Instellingen file #}
sat_detail AS (
    SELECT *
    FROM {{ ref('rv_sat__company_reference_details') }}
    WHERE end_datetime IS null
),

{# Enrich the sat_detail CTE with the description of code for the listed columns below #}
detail AS (
    {% set columns= [
    'SELLOPH', 'SELLADRFT', 'SELLMLST', 'SELLBDM', 'SELLBTM',
    'SIZCBYZTYP', 'SIZLHFDGBR', 'SIZKAM', 'SIZOSBI', 'SIZOSBI2',
    'SIZOSBISEG', 'SIZCHNV', 'SIZCRV', 'SIZKRV', 'SIZKLFT',
    'SIZCFR_SRT', 'SIZCCOREL', 'SIZCZZP', 'SIZCZZPTYP'] %}
    SELECT
        sat_detail.*,
        {{ ci_select_ref_labels(columns) }}
    FROM sat_detail
    {{ ci_lookup_ref_labels(
        identifier='Instellingen',
        columns=columns
    ) }}
),

{# sourced from Company Info Branches file #}
sat_business_activity AS (
    SELECT *
    FROM {{ ref('rv_sat__company_reference_business_activity') }}
    WHERE end_datetime IS null
),

{#
    Filter out deleted rows by only selecting data
    that are in the latest load cycle of Company Info Keyword Categories file
#}
active_keyword_categories AS (
    {{ ci_filter_deleted_rows(
        rv_hub_link = ref('rv_hub__company_reference'),
        rv_xts = ref('rv_xts__company_reference_keyword_categories'),
        pk = 'company_reference_hk',
        tracked_sat_name = 'rv_masat__company_reference_keyword_categories'
        ) }}
),

{#
    The keyword categories data granularity are detailed into level of matched customer.
    However, the values of keywords (afhalen, bezorgen, etc) are same accross the data rows with the same company code.
    Therefore, the data is distincted on the company code level.
    Next to that, we only select data rows that are in the latest load cycle of Company Info Keyword Categories file.
#}
sat_keyword_categories AS (
    SELECT DISTINCT
        sat.company_reference_hk,
        sat.afhalen,
        sat.bezorgen,
        sat.dark_kitchen,
        sat.platformen,
        sat.terras,
        sat.uitsluiten
    FROM {{ ref('rv_masat__company_reference_keyword_categories') }} AS sat
    INNER JOIN active_keyword_categories
        ON
            sat.company_reference_hk = active_keyword_categories.company_reference_hk
            AND sat.hashdiff = active_keyword_categories.hashdiff
    WHERE sat.end_datetime IS null
),

{#
    Capture company hierarchy and get its parent code and ultimate parent code.
    A company may be assigned to different parent or ultimate parent overtime.
    Therefore, we only select the latest assigned hierarchy of the company.
#}
company_hierarchy AS (
    SELECT
        hub_company.company_reference_hk,
        hub_company_parent.company_reference_bk AS company_reference_parent_code,
        hub_ultimate_parent.company_reference_bk AS company_reference_code_ultimate_parent
    FROM {{ ref('rv_hlnk__company_reference_hierarchy') }} AS hierarchy
    LEFT JOIN {{ ref('rv_hub__company_reference') }} AS hub_company
        ON hierarchy.company_reference_hk = hub_company.company_reference_hk
    LEFT JOIN {{ ref('rv_hub__company_reference') }} AS hub_company_parent
        ON hierarchy.parent_company_hk = hub_company_parent.company_reference_hk
    LEFT JOIN {{ ref('rv_hub__company_reference') }} AS hub_ultimate_parent
        ON hierarchy.ultimate_parent_company_hk = hub_ultimate_parent.company_reference_hk
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            hierarchy.company_reference_hk
        ORDER BY hierarchy.load_datetime DESC
    ) = 1
),

{# Capture the latest registered address (BAG) of a company #}
company_address AS (
    SELECT
        link_address.*,
        hub_address.company_address_bk,
        hub_postcode.postcode_bk
    FROM {{ ref('rv_lnk__company_reference_address') }} AS link_address
    INNER JOIN {{ ref('rv_hub__company_reference_address') }} AS hub_address
        ON link_address.company_address_hk = hub_address.company_address_hk
    INNER JOIN {{ ref('rv_hub__postcode') }} AS hub_postcode
        ON link_address.postcode_hk = hub_postcode.postcode_hk
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            link_address.company_reference_hk
        ORDER BY link_address.load_datetime DESC
    ) = 1
),

{# Capture the latest building address (BAG Pand) of a company #}
company_building AS (
    SELECT
        link_building.company_reference_hk,
        hub_building.company_building_bk
    FROM {{ ref('rv_lnk__company_reference_building') }} AS link_building
    INNER JOIN {{ ref('rv_hub__company_reference_building') }} AS hub_building
        ON link_building.company_building_hk = hub_building.company_building_hk
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            link_building.company_reference_hk
        ORDER BY link_building.load_datetime DESC
    ) = 1
),

{#
    Filter out deleted rows by only selecting data
    that are in the latest load cycle of Company Info Postcode4 file
#}
active_postcode_keys AS (
    {{ ci_filter_deleted_rows(
        rv_hub_link = ref('rv_hub__postcode'),
        rv_xts = ref('rv_xts__company_reference_postcode4'),
        pk = 'postcode_hk',
        tracked_sat_name = 'rv_sat__postcode_details'
        ) }}
),

{# Only select data rows that are in the latest load cycle of Company Info Postcode4 file. #}
sat_postcode AS (
    SELECT sat_postcode.*
    FROM {{ ref("rv_sat__postcode_details") }} AS sat_postcode
    INNER JOIN active_postcode_keys
        ON sat_postcode.postcode_hk = active_postcode_keys.postcode_hk
    WHERE sat_postcode.end_datetime IS null
),

{# Enrich the sat_postcode CTE with the description of code for the listed columns below #}
postcode AS (
    {% set columns= ['SIZCVAKREG'] %}
    SELECT
        sat_postcode.*,
        {{ ci_select_ref_labels(columns) }}
    FROM sat_postcode
    {{ ci_lookup_ref_labels(
        identifier='Info_Postcode4',
        columns=columns
    ) }}
),

{# Capture the latest location of company #}
link_location AS (
    SELECT *
    FROM {{ ref('rv_lnk__company_reference_location') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            company_reference_hk
        ORDER BY load_datetime DESC
    ) = 1
),

{#
    Filter out deleted rows by only selecting data
    that are in the latest load cycle of Company Info Sizokey_Opgeheven file
#}
latest_discontinued_company AS (
    {{ ci_filter_deleted_rows(
        rv_hub_link = ref('rv_hub__company_reference'),
        rv_xts = ref('rv_xts__company_reference_sizokeys_opgeheven'),
        pk = 'company_reference_hk',
        tracked_sat_name = 'rv_sat__company_reference_company_keys_deleted'
        ) }}
),

{# Only select data rows that are in the latest load cycle of Company Info Sizokey_Opgeheven file. #}
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
    The company mart covers information of:
    - Details of the company (detail CTE)
    - Hierarchy : company parent code, ultimate parent code (company_hierarchy CTE)
    - Latest registered address-BAG (company_address CTE)
    - Latest registered building-BAG Pand (company_building CTE)
    - Sligro customer category and segment based on SBI code
    - Secondary SBI information based on KvK / coc (chamber of commerce) and Company info analysis (sat_business_activity CTE)
    - Indicator whether the company has been discontinued or not
#}
result AS (
    SELECT
        hub_company.company_reference_bk AS pk_company_reference_code,
        company_hierarchy.company_reference_parent_code AS fk_company_reference_parent_code,
        company_hierarchy.company_reference_code_ultimate_parent AS fk_company_reference_code_ultimate_parent,
        company_address.company_address_bk AS fk_company_reference_property_features_code,
        detail.sizolockey AS fk_company_reference_location_code,
        detail.sizopcd AS postal_code_6_digit,
        company_address.postcode_bk AS postal_code_4_digit,
        detail.sizokvk AS coc_number,
        detail.sizoves AS branch_number,
        detail.sizonm AS company_name,
        detail.sizostn AS statutory_name,
        detail.sizohn1 AS trade_name_1,
        detail.sizohn2 AS trade_name_2,
        detail.sizohn3 AS trade_name_3,
        detail.sizonm_up AS company_reference_ultimate_parent_name,
        detail.sizoadr AS address,
        detail.sizostr AS street_name,
        detail.sizohnr AS house_number,
        detail.sizothn AS house_number_addition,
        detail.sizoplt AS city,
        detail.sizocadr AS correspondence_address,
        detail.sizocstr AS correspondence_street_name,
        detail.sizochnr AS correspondence_house_number,
        detail.sizocthn AS correspondence_house_number_addition,
        detail.sizocpcd AS correspondence_postal_code,
        detail.sizocplt AS correspondence_city,
        detail.sizopbadr AS mailbox_address,
        detail.sizopbpcd AS mailbox_postal_code,
        detail.sizopbplt AS mailbox_city,
        detail.sizotel AS company_info_phone_number,
        detail.sizttel AS phone_number,
        (detail.selladrft = 1) AS business_address_incorrect_indicator,
        (detail.sellmlst = 1) AS mailstop_indicator,
        (detail.sellbdm = 1) AS direct_mail_indicator,
        (detail.sellbtm = 1) AS telemarketing_indicator,
        (detail.sizlbmn = 1) AS natural_person_indicator,
        (detail.sizlio = 1) AS in_formation_indicator,
        detail.sizcbyztyp_desc AS special_business_type,
        (detail.sizlhfdgbr = 1) AS main_user_of_building_indicator,
        detail.sizocilink AS company_info_url,
        detail.sizwam AS number_of_employees,
        detail.sizkam_desc AS number_of_employees_class,
        detail.sizosbi AS sbi_code,
        detail.sizosbi_desc AS sbi_description,
        detail.sizosbi2 AS sbi_code_2_digit,
        detail.sizosbi2_desc AS sbi_code_2_digit_description,
        detail.sizosbiseg AS sbi_code_segment,
        detail.sizosbiseg_desc AS sbi_segment_description,
        ref_sbi_mapping.category_code,
        ref_sbi_mapping.category_description,
        ref_sbi_mapping.segment_code,
        ref_sbi_mapping.segment_description,
        detail.sizchnv_desc AS main_branch_type,
        detail.sizcrv_desc AS legal_form,
        detail.sizdopr AS establishment_date,
        detail.sizwoprjr AS year_of_establishment,
        detail.sizwlft AS company_age,
        detail.sizklft_desc AS company_age_class,
        detail.sizobtw AS vat_number,
        detail.sizourl AS company_url,
        detail.sizofr_frm AS franchise_formula,
        detail.sizcfr_srt_desc AS franchise_type,
        (detail.sizlmdmu_l = 1) AS marketable_location_indicator,
        (detail.sizlmdmu_c = 1) AS marketable_dmu_indicator,
        detail.sizoskdmul AS fk_company_reference_code_marketable_location,
        detail.sizoskdmuc AS fk_company_reference_marketable_dmu,
        (detail.sizlbdmu_l = 1) AS decision_making_on_location_indicator,
        (detail.sizocontlr = 1) AS best_approachable_in_concern_indicator,
        (detail.sizoloctlr = 1) AS best_approachable_on_location_indicator,
        detail.sizwhinivo AS hierarchical_level_within_concern,
        detail.sizccorel_desc AS concern_relationship,
        SAFE_CAST(SUBSTR(detail.sizostart, 1, 4) AS INT64) AS year_started,
        SAFE_CAST(SUBSTR(detail.sizostart, 5, 2) AS INT64) AS month_started,
        detail.sizczzp_desc AS code_freelance,
        detail.sizczzptyp_desc AS code_freelance_type,
        detail.sizdvrh AS moved_date,
        detail.sizdnmw AS name_changed_date,
        sat_business_activity.kvk_hoofd_sbi_code AS coc_main_sbi_code,
        sat_business_activity.kvk_hoofd_sbi_code_omschrijving AS coc_main_sbi_description,
        sat_business_activity.kvk_neven_1_sbi_code AS coc_secondary_sbi_code_1,
        sat_business_activity.kvk_neven_1_sbi_omschrijving AS coc_secondary_sbi_code_1_description,
        sat_business_activity.kvk_neven_2_sbi_code AS coc_secondary_sbi_code_2,
        sat_business_activity.kvk_neven_2_sbi_omschrijving AS coc_secondary_sbi_code_2_description,
        sat_business_activity.kvk_neven_3_sbi_code AS coc_secondary_sbi_code_3,
        sat_business_activity.kvk_neven_3_sbi_omschrijving AS coc_secondary_sbi_code_3_description,
        sat_business_activity.company_info1_code AS company_info_sbi_code_1,
        sat_business_activity.company_info1_omschrijving AS company_info_sbi_code_1_description,
        sat_business_activity.company_info2_code AS company_info_sbi_code_2,
        sat_business_activity.company_info2_omschrijving AS company_info_sbi_code_2_description,
        (sat_keyword_categories.afhalen = 'yes') AS webcrawling_takeaway_indicator,
        (sat_keyword_categories.bezorgen = 'yes') AS webcrawling_delivery_indicator,
        (sat_keyword_categories.dark_kitchen = 'yes') AS webcrawling_dark_kitchen_indicator,
        (sat_keyword_categories.platformen = 'yes') AS webcrawling_platform_indicator,
        (sat_keyword_categories.terras = 'yes') AS webcrawling_terrace_indicator,
        postcode.sizcvakreg_desc AS holiday_region,
        (COALESCE(discontinued_company.selloph, 0) = 1) AS discontinuation_indicator,
        discontinued_company.selcoph_desc AS discontinuation_type,
        discontinued_company.siztophdat AS discontinuation_date
    FROM {{ ref('rv_hub__company_reference') }} AS hub_company
    LEFT JOIN detail
        ON hub_company.company_reference_hk = detail.company_reference_hk
    LEFT JOIN company_address
        ON
            hub_company.company_reference_hk = company_address.company_reference_hk
            AND detail.sizolocidm = company_address.company_address_bk --match latest registered company address
    LEFT JOIN company_building
        ON
            hub_company.company_reference_hk = company_building.company_reference_hk
            AND detail.bagopndid = company_building.company_building_bk --match latest registered company building
    LEFT JOIN company_hierarchy
        ON hub_company.company_reference_hk = company_hierarchy.company_reference_hk
    LEFT JOIN sat_business_activity
        ON hub_company.company_reference_hk = sat_business_activity.company_reference_hk
    LEFT JOIN link_location
        ON hub_company.company_reference_hk = link_location.company_reference_hk
    LEFT JOIN postcode
        ON company_address.postcode_hk = postcode.postcode_hk
    LEFT JOIN sat_keyword_categories
        ON hub_company.company_reference_hk = sat_keyword_categories.company_reference_hk
    LEFT JOIN discontinued_company
        ON hub_company.company_reference_hk = discontinued_company.company_reference_hk
    LEFT JOIN {{ ref('ref__company_sbi_category_segment_mapping') }} AS ref_sbi_mapping
        ON detail.sizosbi = ref_sbi_mapping.sbi_code
    -- Select both active and discontinued companies.
    -- Some data of discontinued companies may not exist in the Instellingen (sat_company_reference)
    WHERE detail.company_reference_hk IS NOT null OR discontinued_company.company_reference_hk IS NOT null
)

SELECT *
FROM result
