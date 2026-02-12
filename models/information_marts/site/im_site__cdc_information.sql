WITH

site_hub AS (
    SELECT
        site_bk,
        site_hk
    FROM {{ ref('rv_hub__site') }}
),

site_cdc_information AS (
    SELECT
        site_hk,
        finrdn,
        dckddn,
        dcomdn,
        adrsdn,
        dcpkdn,
        dcwpdn
    FROM {{ ref('rv_sat__site_cdc_info') }}
    WHERE end_datetime IS null
),

site_hub_satellite_join AS (
    SELECT
        site_hub.site_bk AS fk_site_code,
        CONCAT(site_cdc_information.dckddn, '||', site_hub.site_bk) AS pk_dc_and_site_code,
        site_cdc_information.dckddn AS dc_code,
        INITCAP(site_cdc_information.dcomdn) AS dc_description,
        site_cdc_information.dcpkdn AS postal_code,
        INITCAP(site_cdc_information.adrsdn) AS address,
        INITCAP(site_cdc_information.dcwpdn) AS city
    FROM site_hub
    INNER JOIN site_cdc_information ON site_hub.site_hk = site_cdc_information.site_hk
)

SELECT
    pk_dc_and_site_code,
    fk_site_code,
    dc_code,
    dc_description,
    address,
    postal_code,
    city
FROM site_hub_satellite_join
