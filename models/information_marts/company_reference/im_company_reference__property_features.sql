WITH
{#
    Filter out deleted rows by only selecting data
    that are in the latest load cycle of Company Info BAG file
#}
active_address_keys AS (
    {{ ci_filter_deleted_rows(
        rv_hub_link = ref('rv_hub__company_reference_address'),
        rv_xts = ref('rv_xts__company_reference_bag'),
        pk = 'company_address_hk',
        tracked_sat_name = 'rv_sat__company_reference_address_details'
        ) }}
),

{# Only select data rows that are in the latest load cycle of Company Info BAG file. #}
sat_address AS (
    SELECT
        sat_address.*,
        active_address_keys.company_address_bk
    FROM {{ ref('rv_sat__company_reference_address_details') }} AS sat_address
    INNER JOIN active_address_keys
        ON sat_address.company_address_hk = active_address_keys.company_address_hk
    WHERE sat_address.end_datetime IS null
),

{# Enrich the sat_address CTE with the description of code for the listed columns below #}
address AS (
    {% set columns=['BAGKOPVL', 'BAGCGBDL','BAGCSTPND', 'SIZCLOCTYP',
    'SIZKCONADR', 'PECTYPWO', 'PECCEIG', 'CIENERL'] %}
    SELECT
        sat_address.*,
        {{ ci_select_ref_labels(columns) }}
    FROM sat_address
    {{ ci_lookup_ref_labels(
        identifier='BAG',
        columns=columns
    ) }}
),

{#
    Filter out deleted rows by only selecting data
    that are in the latest load cycle of Company Info BAG Pand file
#}
active_building_keys AS (
    {{ ci_filter_deleted_rows(
        rv_hub_link = ref('rv_hub__company_reference_building'),
        rv_xts = ref('rv_xts__company_reference_bag_pand'),
        pk = 'company_building_hk',
        tracked_sat_name = 'rv_sat__company_reference_building_details'
        ) }}
),

{# Only select data rows that are in the latest load cycle of Company Info BAG Pand file. #}
sat_building AS (
    SELECT
        sat_building.*,
        active_building_keys.company_building_bk
    FROM {{ ref('rv_sat__company_reference_building_details') }} AS sat_building
    INNER JOIN active_building_keys
        ON sat_building.company_building_hk = active_building_keys.company_building_hk
    WHERE sat_building.end_datetime IS null
),

{# Enrich the sat_building CTE with the description of code for the listed columns below #}
building AS (
    {% set columns=['SIZCBEDVER'] %}
    SELECT
        sat_building.*,
        {{ ci_select_ref_labels(columns) }}
    FROM sat_building
    {{ ci_lookup_ref_labels(
        identifier='BAG_Pand',
        columns=columns
    ) }}
),

link_address_building AS (
    -- Select only the latest registered building for an address.
    SELECT *
    FROM {{ ref('rv_lnk__company_reference_building_address') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            company_address_hk
        ORDER BY load_datetime DESC
    ) = 1
),

result AS (
    SELECT
        address.company_address_bk AS pk_company_reference_property_features_code,
        building.company_building_bk AS building_code,
        address.bagwopvl AS surface,
        address.bagkopvl_desc AS surface_class,
        address.bagcgbdl_desc AS main_function_building,
        (address.baglgd1bij = 1) AS meeting_function_indicator,
        (address.baglgd2cel = 1) AS cell_function_indicator,
        (address.baglgd3gez = 1) AS healthcare_function_indicator,
        (address.baglgd4ind = 1) AS industrial_function_indicator,
        (address.baglgd5kan = 1) AS office_function_indicator,
        (address.baglgd6log = 1) AS lodging_function_indicator,
        (address.baglgd7ond = 1) AS educational_function_indicator,
        (address.baglgd8spo = 1) AS sports_function_indicator,
        (address.baglgd9win = 1) AS shopping_function_indicator,
        (address.baglgd10wo = 1) AS residential_function_indicator,
        (address.baglgd0ove = 1) AS other_function_indicator,
        address.bagcstpnd_desc AS building_status,
        address.sizcloctyp_desc AS location_type,
        address.sizwconadr AS number_of_concerns_property,
        address.sizkconadr_desc AS number_of_concerns_property_class,
        address.pectypwo_desc AS house_type,
        address.pecceig_desc AS ownership_type,
        address.pecwwoz AS woz_value,
        address.cienerl_desc AS energy_label,
        building.sizcbedver_desc AS shared_company_building_type,
        building.pecwtuiop AS outside_area_surface,
        address.sizolatit AS coordinate_latitude,
        address.sizolongit AS coordinate_longitude,
        ST_GEOGPOINT(address.sizolatit, address.sizolongit) AS coordinate
    FROM address
    -- an address may not have building information linked to it
    LEFT JOIN link_address_building
        ON address.company_address_hk = link_address_building.company_address_hk
    LEFT JOIN building
        ON link_address_building.company_building_hk = building.company_building_hk
)

SELECT *
FROM result
