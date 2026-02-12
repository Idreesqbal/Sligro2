WITH

sat_details_cdc AS (
    SELECT *
    FROM {{ ref("rv_sat__collection_area_details_cdc") }}
    WHERE end_datetime IS null
),

collection_area_cdc AS (
    SELECT
        -- hub_collection_area
        hub_collection_area.collection_area_nk AS pk_collection_area_code,
        -- hub_site
        hub_site.site_bk AS fk_site_code,
        -- sat_details_cdc
        sat_details_cdc.dckdc1 AS dc_code,
        CAST(sat_details_cdc.areac1 AS STRING) AS area_code,
        sat_details_cdc.daomc1 AS area_description,
        (sat_details_cdc.fvjnc1 = 'Y') AS brochure_indicator,
        'MISSING_UNKNOWN' AS environment,
        'MISSING_UNKNOWN' AS event_return_indicator,
        CAST(null AS BOOLEAN) AS event_indicator
    FROM {{ ref("rv_hub__collection_area") }} AS hub_collection_area
    INNER JOIN sat_details_cdc
        ON hub_collection_area.collection_area_hk = sat_details_cdc.collection_area_hk
    INNER JOIN {{ ref("rv_lnk__collection_area__site") }} AS link_collection_area__site
        ON hub_collection_area.collection_area_hk = link_collection_area__site.collection_area_hk
    INNER JOIN {{ ref("rv_hub__site") }} AS hub_site
        ON link_collection_area__site.site_hk = hub_site.site_hk
),

sat_details_bs_zb AS (
    SELECT *
    FROM {{ ref("rv_sat__collection_area_details_bs_zb") }}
    WHERE end_datetime IS null
),

collection_area_bs_zb AS (
    SELECT
        -- hub_collection_area
        hub_collection_area.collection_area_nk AS pk_collection_area_code,
        -- hub_site
        hub_site.site_bk AS fk_site_code,
        -- sat_details_bs_zb
        CAST(null AS STRING) AS dc_code,
        sat_details_bs_zb.verzamel_gebied_id AS area_code,
        sat_details_bs_zb.verzamel_gebied_oms AS area_description,
        (sat_details_bs_zb.folder_verdeling = 'Y') AS brochure_indicator,

        'MISSING_UNKNOWN' AS environment,
        'MISSING_UNKNOWN' AS event_return_indicator,
        (sat_details_bs_zb.verzamel_gebied_id = 'EVT') AS event_indicator


    FROM {{ ref('rv_hub__collection_area') }} AS hub_collection_area
    INNER JOIN sat_details_bs_zb
        ON hub_collection_area.collection_area_hk = sat_details_bs_zb.collection_area_hk
    INNER JOIN {{ ref('rv_lnk__collection_area__site') }} AS link_collection_area__site
        ON hub_collection_area.collection_area_hk = link_collection_area__site.collection_area_hk
    INNER JOIN {{ ref('rv_hub__site') }} AS hub_site
        ON link_collection_area__site.site_hk = hub_site.site_hk
),

unioning AS (
    {% set list_of_columns = [
        'pk_collection_area_code',
        'fk_site_code',
        'dc_code',
        'area_code',
        'area_description',
        'brochure_indicator',
        'environment',
        'event_return_indicator',
        'event_indicator'
    ] -%}
    {% set list_of_ctes = ['collection_area_cdc', 'collection_area_bs_zb'] -%}
    {% for cte in list_of_ctes %}
        SELECT {{ list_of_columns | join(', ') }}
        FROM {{ cte }}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {%- endfor %}
)

SELECT * FROM unioning
