WITH
{#
    Filter out deleted rows by only selecting data
    that are in the latest load cycle of Company Info Contactpersonen file
#}
active_contact_person_keys AS (
    {{ ci_filter_deleted_rows(
        rv_hub_link = ref('rv_hub__company_reference'),
        rv_xts = ref('rv_xts__company_reference_contactpersonen'),
        pk = 'company_reference_hk',
        tracked_sat_name = 'rv_masat__company_reference_contact_person'
        ) }}
),

{# Only select data rows that are in the latest load cycle of Company Info Contactpersonen file. #}
sat_contact_person AS (
    SELECT
        *,
        DATE_DIFF(CURRENT_DATE(), geboortedatum, YEAR)
        - IF(FORMAT_DATE('%m%d', geboortedatum) > FORMAT_DATE('%m%d', CURRENT_DATE()), 1, 0) AS age
    FROM {{ ref("rv_masat__company_reference_contact_person") }}
    WHERE end_datetime IS null
),

{# Enrich the sat_contact_person CTE with the description of code for the listed columns below #}
contact_person AS (
    {% set columns= ['SIZCPFNC', 'SIZOPSEX'] %}
    SELECT
        sat_contact_person.*,
        {{ ci_select_ref_labels(columns) }}
    FROM sat_contact_person
    {{ ci_lookup_ref_labels(
        identifier='Contactpersonen',
        columns=columns
    ) }}
),

result AS (
    SELECT
        CONCAT(active_contact_person_keys.company_reference_bk, '||', contact_person.sizocpkey)
            AS pk_company_reference_contact_person_code,
        active_contact_person_keys.company_reference_bk AS fk_company_reference_code,
        contact_person.sizcpfnc_desc AS function_description,
        contact_person.sizopsex_desc AS gender,
        contact_person.sizoptit AS title,
        contact_person.sizopvl AS initials,
        contact_person.sizoptvg AS prefix,
        contact_person.sizopnm AS surname,
        contact_person.sizoptvgnm AS prefix_with_surname,
        (contact_person.sizlcp = 1) AS main_contact_person_indicator,
        contact_person.geboortedatum AS date_of_birth,
        contact_person.age
    FROM active_contact_person_keys
    -- select only latest active contact person from the multiactive satellite
    INNER JOIN contact_person
        ON
            active_contact_person_keys.company_reference_hk = contact_person.company_reference_hk
            AND active_contact_person_keys.hashdiff = contact_person.hashdiff
)

SELECT *
FROM result
