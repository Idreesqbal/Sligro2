WITH

site_hub AS (
    SELECT
        site_bk,
        site_hk
    FROM {{ ref('rv_hub__site') }}
),

site_details AS (
    SELECT
        site_hk,
        naamc1,
        adrsc1,
        pskdc1,
        wnplc1,
        s006c1,
        finrc4,
        von5c1,
        srk5c1,
        ROW_NUMBER() OVER (
            PARTITION BY site_hk
            ORDER BY load_datetime DESC
        ) AS rank_num
    FROM {{ ref('rv_sat__site_details') }}
    QUALIFY rank_num = 1 --includes deleted sites as well
),

site_headoffice_link AS (
    SELECT
        site_hk,
        headoffice_hk
    FROM {{ ref('rv_hlnk__site_headoffice') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY site_hk ORDER BY load_datetime DESC) = 1
),

site_hub_link_satellite_join AS (
    SELECT
        site_hub.site_bk AS pk_site_code,
        headoffice_hub.site_bk AS headoffice_code,
        site_details.pskdc1 AS postal_code,
        site_details.s006c1 AS site_status,
        IF(site_details.s006c1 = 1, "Active", "Inactive") AS site_status_description,
        INITCAP(site_details.naamc1) AS site,
        INITCAP(REGEXP_EXTRACT(site_details.naamc1, r"^\s*(?:SLIGRO(?:-M)?\s*)?(.*?\S)?\s*$")) AS site_short,
        INITCAP(site_details.adrsc1) AS address,
        INITCAP(site_details.wnplc1) AS city,
        INITCAP(ho_details.naamc1) AS headoffice,
        {{ headquarter_to_country('site_details.FINRC4') }} AS country,
        CASE
            WHEN site_details.von5c1 = "1" THEN "ZB"
            WHEN site_details.von5c1 = "2" THEN "BS"
            WHEN site_details.von5c1 = "3" THEN "Other"
        END AS operating_company,
        UPPER(site_details.srk5c1) AS revenue_class
    FROM site_hub
    LEFT JOIN site_headoffice_link AS lnk
        ON site_hub.site_hk = lnk.site_hk
    INNER JOIN site_details AS site_details
        ON lnk.site_hk = site_details.site_hk
    LEFT JOIN site_hub AS headoffice_hub
        ON lnk.headoffice_hk = headoffice_hub.site_hk
    LEFT JOIN site_details AS ho_details
        ON headoffice_hub.site_hk = ho_details.site_hk

)

/*"Missing" indicates that no source was found for the FK and links need to be created afterwards.
 "Unkown" indicates that the attribute is in the logical data model but it is not present in the source tables.*/
SELECT
    pk_site_code,
    "Missing" AS fk_cost_centre_code,
    "Missing" AS fk_profit_centre_code,
    "Missing" AS fk_building_code,
    "Missing" AS fk_contact_person_code,
    "Missing" AS fk_operational_manager_code,
    site,
    site_short,
    site_status,
    site_status_description,
    address,
    postal_code,
    city,
    "Unknown" AS municipality,
    "Unknown" AS province,
    country,
    "Unknown" AS latitude,
    "Unknown" AS longitude,
    "Unknown" AS site_type,
    revenue_class,
    operating_company,
    "Unknown" AS ownership_status,
    headoffice_code,
    headoffice
FROM site_hub_link_satellite_join
